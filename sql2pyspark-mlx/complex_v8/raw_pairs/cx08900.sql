WITH prepared_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        interests,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket,
        ABS(birth_year - 1981) AS birth_year_distance
    FROM listeners
    WHERE birth_year <= 1988
      AND NOT ARRAY_CONTAINS(interests, 'indie')
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        interests,
        birth_year_bucket,
        birth_year_distance,
        UPPER(TRIM(username)) AS username_clean,
        SIZE(interests) AS interests_count
    FROM prepared_listeners
    WHERE signup_date IS NOT NULL
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM enriched
    WHERE birth_year < (SELECT MAX(birth_year) FROM enriched)
)
SELECT DISTINCT birth_year
FROM qualified
ORDER BY birth_year DESC
