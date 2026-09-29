WITH recent_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        birth_year,
        interests,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek
    FROM listeners
),
unique_rows AS (
    -- Keep the earliest row per plan
    SELECT
        id,
        plan,
        birth_year,
        interests,
        signup_date_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY plan ORDER BY signup_date_dayofweek, id) AS recency_rank
        FROM recent_listeners
    ) ranked
    WHERE recency_rank = 1
),
unique_rows_2 AS (
    -- Keep the latest row per plan
    SELECT
        id,
        plan,
        birth_year,
        interests,
        signup_date_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY plan ORDER BY birth_year DESC, id DESC) AS recency_rank
        FROM unique_rows
    ) ranked
    WHERE recency_rank = 1
),
plan_stats AS (
    -- Max birth_year per plan
    SELECT plan, MAX(birth_year) AS max_birth_year
    FROM unique_rows_2
    GROUP BY plan
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_birth_year,
        ROUND(s.birth_year - g.max_birth_year, 2) AS birth_year_minus_max
    FROM unique_rows_2 s
    JOIN plan_stats g ON g.plan = s.plan
    WHERE s.birth_year = g.max_birth_year
)
-- Subtotals with ROLLUP
SELECT
    plan,
    signup_date_dayofweek,
    ROUND(SUM(birth_year_minus_max), 2) AS total_birth_year_minus_max,
    ROUND(AVG(birth_year), 2) AS avg_birth_year,
    MIN(birth_year_minus_max) AS min_birth_year_minus_max
FROM compared
GROUP BY ROLLUP(plan, signup_date_dayofweek)
