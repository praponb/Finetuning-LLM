WITH cleaned_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        birth_year,
        interests,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown,
        SUBSTRING(TRIM(country), 1, 3) AS country_prefix,
        ABS(birth_year - 1981) AS birth_year_distance
    FROM listeners
    WHERE signup_date BETWEEN DATE '2024-07-13' AND DATE '2025-05-05'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        username,
        plan,
        country,
        birth_year,
        interests,
        plan_or_unknown,
        country_prefix,
        birth_year_distance,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country,
        birth_year_distance - LAG(birth_year_distance) OVER (PARTITION BY plan_or_unknown ORDER BY birth_year, id) AS birth_year_distance_change,
        NTH_VALUE(birth_year_distance, 2) OVER (PARTITION BY country, plan ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year_distance,
        FIRST_VALUE(birth_year) IGNORE NULLS OVER (PARTITION BY country, plan ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_birth_year
    FROM cleaned_listeners
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND MONTH(p.played_at) IN (1, 2)
              AND p.source = 'radio'
        )
)
-- Subtotals with CUBE
SELECT
    COALESCE(plan, 'ALL') AS plan,
    COALESCE(country, 'ALL') AS country,
    COUNT(*) FILTER (WHERE username IS NOT NULL) AS n_username_is_not_null,
    COUNT(DISTINCT plan_or_unknown) AS distinct_plan_or_unknowns
FROM filtered
GROUP BY CUBE(plan, country)
