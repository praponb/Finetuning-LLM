WITH filtered_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM listeners
    WHERE (signup_date BETWEEN DATE '2024-08-01' AND DATE '2025-09-26' OR birth_year <= 1980)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', signup_date) AS signup_date_year,
        ABS(birth_year - 1980) AS birth_year_distance,
        CASE WHEN birth_year >= 1990 THEN 'A' WHEN birth_year >= 1975 THEN 'B' ELSE 'C' END AS birth_year_tier
    FROM filtered_listeners
    WHERE birth_year >= 1967
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        username,
        country,
        signup_date_year,
        birth_year_distance,
        birth_year_tier,
        SUBSTRING(TRIM(birth_year_tier), 1, 3) AS birth_year_tier_prefix,
        SUBSTRING(TRIM(country), 1, 4) AS country_prefix,
        DATEDIFF(DATE '2026-01-01', signup_date_year) AS days_since_signup_date_year
    FROM tagged
    WHERE plan IN ('premium', 'student', 'free')
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(birth_year_tier), 1, 3) AS birth_year_tier_prefix_2,
        DATEDIFF(DATE '2025-12-31', signup_date_year) AS days_since_signup_date_year_2,
        CONCAT(birth_year_tier_prefix, '#', CAST(birth_year_distance AS STRING)) AS birth_year_tier_prefix_ref
    FROM labeled
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT signup_date_year, country, birth_year_tier, birth_year_distance
    FROM tagged_2
) src
PIVOT (
    MIN(birth_year_distance)
    FOR birth_year_tier IN ('A', 'B', 'C')
)
ORDER BY signup_date_year, country
