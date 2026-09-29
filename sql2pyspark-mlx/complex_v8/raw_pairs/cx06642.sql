WITH filtered AS (
    -- Filter listeners
    SELECT
        id,
        username,
        country,
        birth_year,
        interests,
        SUBSTRING(TRIM(username), 1, 4) AS username_prefix
    FROM listeners
    WHERE birth_year <= 1990
      AND plan NOT IN ('student', 'premium')
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM filtered
    WHERE birth_year > (SELECT MIN(birth_year) FROM filtered)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(birth_year) OVER (PARTITION BY country ORDER BY birth_year DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS birth_year_sum_3_prior,
        SUM(birth_year) OVER (PARTITION BY country ORDER BY birth_year NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS birth_year_sum_2,
        MIN(birth_year) OVER (PARTITION BY country) AS country_min_birth_year,
        LEAD(birth_year) OVER (PARTITION BY country ORDER BY birth_year DESC, id) AS next_birth_year,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country
    FROM with_activity
)
-- Final projection
SELECT
    id,
    country,
    birth_year,
    username_prefix,
    birth_year_sum_3_prior,
    country_min_birth_year,
    rows_in_country,
    LENGTH(TRIM(username_prefix)) AS username_prefix_len
FROM windowed
ORDER BY username_prefix DESC, username_prefix_len DESC
