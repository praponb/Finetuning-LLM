WITH scoped_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        interests,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'kpop')
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(birth_year - AVG(birth_year) OVER (PARTITION BY YEAR(signup_date)), 2) AS birth_year_vs_signup_date_year_avg,
        DENSE_RANK() OVER w_all_by_birth_year_bucket_tie AS birth_year_bucket_dense_rank,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER w_signup_date_year, 0), 4) AS birth_year_share_of_signup_date_year,
        ROW_NUMBER() OVER (ORDER BY birth_year DESC, id) AS rn
    FROM scoped_listeners
    WINDOW w_all_by_birth_year_bucket_tie AS (ORDER BY birth_year_bucket DESC), w_signup_date_year AS (PARTITION BY YEAR(signup_date))
)
-- Final projection
SELECT
    id,
    username,
    signup_date,
    interests,
    birth_year_bucket,
    birth_year_bucket_dense_rank,
    rn
FROM metrics
WHERE ARRAY_CONTAINS(interests, 'indie')
