-- Creating table
CREATE TABLE IF NOT EXISTS `coviddata` (
  `iso_code` text,
  `continent` text,
  `location` text,
  `date` text,
  `total_cases` bigint DEFAULT NULL,
  `new_cases` int DEFAULT NULL,
  `total_deaths` bigint DEFAULT NULL,
  `new_deaths` int DEFAULT NULL,
  `total_vaccinations` bigint DEFAULT NULL,
  `people_vaccinated` bigint DEFAULT NULL,
  `people_fully_vaccinated` bigint DEFAULT NULL,
  `total_boosters` bigint DEFAULT NULL,
  `new_vaccinations` bigint DEFAULT NULL,
  `population_density` double DEFAULT NULL,
  `gdp_per_capita` double DEFAULT NULL,
  `handwashing_facilities` double DEFAULT NULL,
  `hospital_beds_per_thousand` double DEFAULT NULL,
  `human_development_index` double DEFAULT NULL,
  `population` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Load data

SHOW GLOBAL VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = true;

LOAD DATA LOCAL INFILE 'C:/Users/ASUS/Downloads/owid-covid-data.csv' INTO TABLE coviddata
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


-- DATA CLEANING
-- 1. fixing datetime format
SELECT STR_TO_DATE(date, '%m/%d/%Y')
FROM coviddata;

ALTER TABLE coviddata
ADD thedate date;

UPDATE coviddata
SET thedate = STR_TO_DATE(date, '%m/%d/%Y');

ALTER TABLE coviddata
DROP COLUMN `date`;

-- 2. filter non countries (world, africa, etc)
SELECT 
	iso_code,
    location
FROM coviddata
WHERE iso_code LIKE 'owid%'
GROUP BY 1,2;

DELETE FROM coviddata
WHERE iso_code LIKE 'owid%';

-- 3. normalizing country dependent stats
CREATE TABLE countries AS
SELECT 
	iso_code,
	location,
    continent,
	population_density, 
    gdp_per_capita, 
    handwashing_facilities, 
    hospital_beds_per_thousand, 
    human_development_index,
    population
FROM coviddata
GROUP BY iso_code, location;

ALTER TABLE coviddata
DROP continent,
DROP population_density, 
DROP gdp_per_capita, 
DROP handwashing_facilities, 
DROP hospital_beds_per_thousand, 
DROP human_development_index,
DROP population;

-- Making views
-- countries total stats
DROP VIEW IF EXISTS popcasdeavac;
CREATE VIEW popcasdeavac AS
SELECT 
	o.iso_code, 
    o.location,
    c.population,
    max(o.total_cases) cases,
    max(o.total_deaths) deaths,
    max(people_vaccinated) fvac
FROM coviddata o
LEFT JOIN countries c
	USING (iso_code)
GROUP BY 1,2;

-- global stats
SELECT 
	sum(population) global_pop,
	sum(cases) global_cases,
    sum(deaths) global_deaths,
    sum(fvac) global_vacc
FROM popcasdeavac;

-- countries rates
WITH rates AS (
SELECT 
	iso_code,
    location,
    population,
    cases,
	cases/population caser,
    deaths/cases deathr,
    fvac/population vaccr
FROM popcasdeavac)
SELECT
	iso_code,
    location,
    caser,
	caser-2*sqrt(caser*(1-caser)/population) lb_caser,
    deathr,
    deathr-2*sqrt(deathr*(1-deathr)/cases) lb_deathr,
    vaccr,
    vaccr-2*sqrt(vaccr*(1-vaccr)/population) lb_vaccr
FROM rates 
ORDER BY lb_deathr DESC;

-- emulating WHO's dashboard
-- cases & deaths by countries
SELECT
	iso_code,
    location,
    cases,
    deaths
FROM popcasdeavac;

-- vaccination 
SELECT
	iso_code,
    location,
    max(total_vaccinations) vac_doses,
    max(people_fully_vaccinated) vac_ppl,
    min(case when total_vaccinations > 0 then thedate end) first_vacc_date
FROM coviddata
GROUP BY 1,2
-- HAVING first_vacc_date IS NOT NULL
-- ORDER BY first_vacc_date
;

-- global numbers
SELECT
	sum(cases) global_cases,
    sum(deaths) global_deaths
FROM popcasdeavac;

-- time axis
SELECT 
	thedate,
    sum(new_cases) daily_cases,
    sum(new_deaths) daily_deaths,
    sum(total_cases) cud_cases,
    sum(total_deaths) cud_deaths
FROM coviddata
GROUP BY 1
ORDER BY 1;

-- weekly
WITH datesummary AS (
SELECT 
	thedate,
    yearweek (thedate) yearweek,
    sum(new_cases) daily_cases,
    sum(new_deaths) daily_deaths,
    sum(total_cases) cud_cases,
    sum(total_deaths) cud_deaths
FROM coviddata
GROUP BY 1 )
SELECT 
	yearweek,
    STR_TO_DATE(CONCAT(yearweek,' Monday'), '%X%V %W') mondaydate,
    sum(daily_cases) weekly_cases,
    sum(daily_deaths) weekly_deaths,
    max(cud_cases) cuw_cases,
    max(cud_deaths) cuw_deats
FROM datesummary
GROUP BY 1
ORDER BY 1;

-- by region (or continent)
-- summary cases & deaths (&vaccinated people, might as well)
SELECT
	c.continent,
    sum(p.cases) cases,
    sum(p.deaths) deaths,
    sum(p.fvac) fvac
FROM popcasdeavac p
LEFT JOIN countries c
	USING (iso_code)
GROUP BY 1
ORDER BY 2 DESC;

-- time axis by continent
SELECT 
	d.thedate,
    c.continent,
    sum(d.new_cases) daily_cases,
    sum(d.new_deaths) daily_deaths,
    sum(d.total_cases) cud_cases,
    sum(d.total_deaths) cud_deaths
FROM coviddata d
LEFT JOIN countries c
	USING (iso_code)
GROUP BY 1,2
ORDER BY 1;

-- weekly
WITH datesummary AS (
SELECT 
	d.thedate,
    yearweek (d.thedate) yearweek,
    c.continent,
    sum(d.new_cases) daily_cases,
    sum(d.new_deaths) daily_deaths,
    sum(d.total_cases) cud_cases,
    sum(d.total_deaths) cud_deaths
FROM coviddata d
LEFT JOIN countries c
	USING (iso_code)
GROUP BY 1,3 )
SELECT 
	yearweek,
    STR_TO_DATE(CONCAT(yearweek,' Monday'), '%X%V %W') mondaydate,
    continent,
    sum(daily_cases) weekly_cases,
    sum(daily_deaths) weekly_deaths,
    max(cud_cases) cuw_cases,
    max(cud_deaths) cuw_deats
FROM datesummary
GROUP BY 1,3
-- HAVING continent = '(choose continent)'
ORDER BY 3,1;
