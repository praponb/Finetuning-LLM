WITH cleaned AS (
    -- Filter listeners
    SELECT
        id,
        username,
        country,
        birth_year,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM listeners
    WHERE NOT ARRAY_CONTAINS(interests, 'podcasts')
)
SELECT username AS category
FROM cleaned
WHERE birth_year_bucket > 1979
  AND birth_year <= 1980
EXCEPT
SELECT username
FROM listeners
WHERE birth_year < 1986
  AND UPPER(TRIM(username)) LIKE 'E%'
ORDER BY category
