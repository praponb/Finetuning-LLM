WITH base AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE (plan NOT IN ('student', 'free') OR signup_date >= DATE '2024-09-19')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(signup_date, 2) OVER (ORDER BY signup_date DESC, id) AS prev2_signup_date,
        ROUND(CUME_DIST() OVER (ORDER BY signup_date, id), 3) AS signup_date_cume_dist,
        COUNT(*) OVER w_plan_country AS rows_in_plan_country,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY signup_date, id) AS rn,
        COUNT(*) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(birth_year) OVER w_plan_country AS plan_country_max_birth_year,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY plan ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        NTH_VALUE(birth_year, 2) OVER (PARTITION BY plan ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year,
        RANK() OVER (ORDER BY signup_date, id) AS signup_date_rank
    FROM base
    WINDOW w_plan_country AS (PARTITION BY plan, country)
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(second_birth_year) OVER (ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_second_birth_year,
        COUNT(*) OVER w_signup_date_rank AS rows_in_signup_date_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_signup_date) ORDER BY prev2_signup_date) AS prev2_signup_date_rank,
        ROUND(CUME_DIST() OVER (ORDER BY signup_date DESC), 3) AS signup_date_cume_dist_2,
        ROUND(AVG(running_count) OVER (PARTITION BY DATE_TRUNC('MONTH', first_known_signup_date) ORDER BY prev2_signup_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS running_count_avg_6_prior,
        DATEDIFF(signup_date, LAG(signup_date) OVER w_all_by_signup_date) AS days_since_prev_signup_date,
        MIN(running_count) OVER (PARTITION BY signup_date_rank) AS signup_date_rank_min_running_count
    FROM metrics
    WINDOW w_signup_date_rank AS (PARTITION BY signup_date_rank), w_all_by_signup_date AS (ORDER BY signup_date DESC, id)
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
    prev2_signup_date,
    signup_date_cume_dist,
    rows_in_plan_country,
    rn,
    first_known_signup_date,
    second_birth_year,
    signup_date_rank,
    last_second_birth_year,
    rows_in_signup_date_rank,
    signup_date_cume_dist_2,
    running_count_avg_6_prior,
    days_since_prev_signup_date,
    signup_date_rank_min_running_count,
    ARRAY_CONTAINS(interests, 'kpop') AS has_kpop
FROM metrics_2
ORDER BY plan
