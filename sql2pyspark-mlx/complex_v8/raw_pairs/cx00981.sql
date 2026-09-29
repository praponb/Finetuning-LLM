WITH valid_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests,
        DATE_ADD(signup_date, 30) AS signup_date_plus_30d,
        LENGTH(TRIM(username)) AS username_len
    FROM listeners
    WHERE signup_date IS NOT NULL
      AND country = 'UK'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        username,
        plan,
        birth_year,
        interests,
        username_len,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY YEAR(signup_date), username_len ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(signup_date_plus_30d) ORDER BY birth_year DESC), 3) AS birth_year_percent_rank
    FROM valid_listeners
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        plan,
        interests,
        username_len,
        first_known_signup_date,
        birth_year_percent_rank,
        FIRST_VALUE(username_len) OVER (PARTITION BY plan ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_username_len,
        COUNT(*) OVER (PARTITION BY plan) AS rows_in_plan
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, rows_in_plan, first_username_len
    FROM with_trends
) src
UNPIVOT (
    reading FOR metric_name IN (rows_in_plan, first_username_len)
) unp
