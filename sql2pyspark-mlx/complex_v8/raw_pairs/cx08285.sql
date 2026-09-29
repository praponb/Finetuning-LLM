WITH valid AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM listeners
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY plan ORDER BY birth_year_bucket, id), 3) AS birth_year_bucket_percent_rank,
        birth_year - LAG(birth_year) OVER (PARTITION BY plan ORDER BY birth_year_bucket, id) AS birth_year_change,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER w_plan_country, 0), 4) AS birth_year_share_of_plan_country,
        MIN(birth_year) OVER (PARTITION BY plan, country) AS plan_country_min_birth_year,
        NTILE(10) OVER (PARTITION BY plan ORDER BY signup_date, id) AS signup_date_decile,
        MIN(birth_year_bucket) OVER (PARTITION BY plan, country) AS plan_country_min_birth_year_bucket,
        COUNT(*) OVER (PARTITION BY plan, country) AS rows_in_plan_country,
        LAST_VALUE(birth_year_bucket) IGNORE NULLS OVER (PARTITION BY plan ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_birth_year_bucket,
        LAST_VALUE(birth_year_bucket) OVER (PARTITION BY plan ORDER BY birth_year_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_birth_year_bucket,
        NTH_VALUE(birth_year_bucket, 2) OVER (PARTITION BY plan ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year_bucket,
        ROUND(birth_year_bucket / NULLIF(SUM(birth_year_bucket) OVER (PARTITION BY plan, country), 0), 4) AS birth_year_bucket_share_of_plan_country,
        MAX(birth_year) OVER w_plan_country AS plan_country_max_birth_year,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY birth_year_bucket) AS birth_year_bucket_dense_rank,
        ROUND(AVG(birth_year) OVER (PARTITION BY YEAR(signup_date) ORDER BY signup_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS birth_year_avg_1_prior,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY YEAR(signup_date) ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date
    FROM valid
    WINDOW w_plan_country AS (PARTITION BY plan, country)
)
-- Final projection
SELECT
    id,
    username,
    plan,
    country,
    signup_date,
    birth_year,
    interests,
    birth_year_bucket,
    birth_year_bucket_percent_rank,
    birth_year_change,
    birth_year_share_of_plan_country,
    plan_country_min_birth_year,
    signup_date_decile,
    last_birth_year_bucket,
    second_birth_year_bucket,
    birth_year_bucket_share_of_plan_country,
    birth_year_bucket_dense_rank,
    birth_year_avg_1_prior,
    second_signup_date,
    SUBSTRING(TRIM(username), 1, 3) AS username_prefix
FROM ranked
ORDER BY plan_country_min_birth_year NULLS LAST, birth_year_change DESC
