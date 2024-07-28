-- Create a new database/Schema called "Big_Coporations" --
# DOWNLOAD the raw dataset (https://tinyurl.com/mwvfv995)
# DOWLOAD the cleaned datset (https://tinyurl.com/mabcub43)


/*
Covid 19 Data Exploration Project

Skills used: Joins, CTE's,  Windows Functions, Number Functions, Converting Data Types

*/

#Sceondly use table data import wizard to import the cvs file to the new craeted database "layoffs_data"

# lets see what we got
SELECT *
FROM layoffs_data
;

# As a case study, I create a tempoaray table to visualize the data for the United States
DROP temporary TABLE IF EXISTS nkem_table;
CREATE TEMPORARY TABLE nkem_table
SELECT *
FROM layoffs_data
WHERE country = 'United states'
;

SELECT *
FROM nkem_table
;

-- CLEANING EXPLORATION 
# create staging table 
# Remve duplicate
# standardise the dataset
# Null values or Bank row
# Remove unwanted column and rows

SELECT *
FROM layoffs_data
;

-- STEP ONE --
#CREAT STAGGING TABLE#
# The best way to carry out cleaning is to create a stagging table rather than using raw tables
DROP table IF EXISTS layoffs_stagging;
CREATE TABLE layoffs_stagging
LIKE layoffs_data
;


SELECT *
FROM layoffs_stagging
;

INSERT INTO layoffs_stagging
SELECT *
FROM layoffs_data
;

SELECT *
FROM layoffs_stagging
;


# STEP TWO #
-- REMOVE DUPLICATE --
# due do there is no uniqu identifier we will asign row number, any row number greater than one is a duplicated value 

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_stagging
ORDER BY company
;



-- Querying the row number as unique identifier for duplicate we need CTE

WITH duplicate_cte AS 
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_stagging
ORDER BY company
) 
SELECT * 
FROM duplicate_cte
WHERE row_num > 1
;



-- CTE does not allow update, we will crate a new table to allow us to make an update 
DROP TABLE IF EXISTS layoffs_stagging2;

CREATE TABLE `layoffs_stagging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT *
FROM layoffs_stagging2
;



INSERT INTO layoffs_stagging2
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_stagging
ORDER BY company
;



SELECT *
FROM layoffs_stagging2
WHERE row_num > 1
;



# dele duplicate
DELETE
FROM layoffs_stagging2
WHERE row_num > 1
;



-- STEP THREE --
# STANDARDISATION/STANDARDISING DATA #
# is find issue in your data and fix it

SELECT *
FROM layoffs_stagging2
;


-- use Trim function to allign the values in sandard format
SELECT DISTINCT company, TRIM(company)
FROM layoffs_stagging2
;


UPDATE layoffs_stagging2
SET company = TRIM(company)
;


SELECT DISTINCT industry
FROM layoffs_stagging2
ORDER BY 1
;

-- find and replace some incorrectly spelled word in company column
SELECT industry
FROM layoffs_stagging2
WHERE industry LIKE 'Crypto%'
;



UPDATE layoffs_stagging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;


# check for update
SELECT DISTINCT industry
FROM layoffs_stagging2
ORDER BY industry
;


SELECT country
FROM layoffs_stagging2
ORDER BY 1
;


SELECT country, TRIM(TRAILING '.' FROM country) AS label
FROM layoffs_stagging2
ORDER BY 1
;


UPDATE  layoffs_stagging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;


SELECT DISTINCT country
FROM layoffs_stagging2
ORDER BY 1
;

# Covert to time series (from text to date)
SELECT`date`,
STR_TO_DATE(`date`, '%m/%d/%Y') 
FROM layoffs_stagging2
;


UPDATE  layoffs_stagging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') 
;


ALTER TABLE layoffs_stagging2
MODIFY COLUMN `date` DATE
;


SELECT  *
FROM layoffs_stagging2
;


-- STEP FOUR --
# REMOVE NULL and Blank Rows
SELECT  *
FROM layoffs_stagging2
WHERE industry IS NULL
OR industry = ''
;

# convert the blacks to NULL VALUES
UPDATE layoffs_stagging2
SET industry = NULL 
WHERE industry = ''
;


-- use self join to pouplate the missing data
SELECT *
FROM layoffs_stagging2 t1
INNER JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
;


SELECT t1.industry, t2.industry
FROM layoffs_stagging2 t1
INNER JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
WHERE t1.industry is NULL 
AND t2.industry IS NOT NULL 
;


# to populate table one 't1' we need to update t1 with t2
UPDATE layoffs_stagging2 t1
INNER JOIN layoffs_stagging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry is NULL 
AND t2.industry IS NOT NULL 
;



SELECT *
FROM layoffs_stagging2
WHERE industry IS NULL
;


# we delet irrevant blanks for instance where the percentage_laid_off is blank and tota_laid_off is blank
SELECT *
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE
FROM layoffs_stagging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_stagging2
;

-- STEP FIVE--
#REMOVE UNSEFUL COLUMNS
# we need to get rid of the row_num because us not valuable to us anymore 
ALTER TABLE layoffs_stagging2
DROP COLUMN row_num
;

SELECT *
FROM layoffs_stagging2
;
-- we can go futher to replace the NULL Values with values of our choice depends on individuals
-- Now we have complete clean data ready for analytics and visualisation 