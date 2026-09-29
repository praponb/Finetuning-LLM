WITH filtered AS (
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
    WHERE plan IN ('premium', 'family', 'free')
      AND birth_year BETWEEN 1967 AND 1994
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        SUM(birth_year) OVER (PARTITION BY country, plan ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_birth_year,
        ROUND(birth_year - AVG(birth_year) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), country), 2) AS birth_year_vs_signup_date_month_country_avg,
        SUM(birth_year) OVER (ORDER BY signup_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS birth_year_sum_7,
        SIZE(COLLECT_SET(plan) OVER w_signup_date_month_country) AS distinct_plan_in_signup_date_month_country,
        LAG(birth_year, 1, 0) OVER w_plan_country_by_signup_date AS prev_birth_year,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date), country) AS rows_in_signup_date_month_country,
        ROUND(AVG(birth_year) OVER w_signup_date_month_country, 2) AS signup_date_month_country_avg_birth_year,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY country, plan), 0), 4) AS birth_year_share_of_country_plan,
        SUM(birth_year) OVER (PARTITION BY country, plan) AS country_plan_sum_birth_year,
        DENSE_RANK() OVER (PARTITION BY plan, country ORDER BY signup_date, id) AS signup_date_dense_rank,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY plan ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        ROUND(birth_year - AVG(birth_year) OVER (PARTITION BY country, plan), 2) AS birth_year_vs_country_plan_avg,
        COUNT(*) OVER (PARTITION BY country, plan) AS rows_in_country_plan,
        ROUND(AVG(birth_year) OVER (ORDER BY birth_year DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS birth_year_avg_2_prior,
        ROUND(PERCENT_RANK() OVER w_country_plan_by_signup_date_2, 3) AS signup_date_percent_rank
    FROM filtered
    WINDOW w_signup_date_month_country AS (PARTITION BY DATE_TRUNC('MONTH', signup_date), country), w_plan_country_by_signup_date AS (PARTITION BY plan, country ORDER BY signup_date, id), w_country_plan_by_signup_date_2 AS (PARTITION BY country, plan ORDER BY signup_date DESC)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        running_birth_year,
        birth_year_vs_signup_date_month_country_avg,
        birth_year_sum_7,
        distinct_plan_in_signup_date_month_country,
        prev_birth_year,
        rows_in_signup_date_month_country,
        signup_date_month_country_avg_birth_year,
        birth_year_share_of_country_plan,
        country_plan_sum_birth_year,
        first_known_signup_date,
        birth_year_vs_country_plan_avg,
        rows_in_country_plan,
        birth_year_avg_2_prior,
        signup_date_percent_rank
    FROM with_windows
    WHERE signup_date_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    username,
    plan,
    country,
    interests,
    running_birth_year,
    birth_year_vs_signup_date_month_country_avg,
    distinct_plan_in_signup_date_month_country,
    rows_in_signup_date_month_country,
    signup_date_month_country_avg_birth_year,
    birth_year_share_of_country_plan,
    country_plan_sum_birth_year,
    first_known_signup_date,
    birth_year_avg_2_prior,
    signup_date_percent_rank,
    DATE_ADD(signup_date, 30) AS signup_date_plus_30d,
    DATE_TRUNC('QUARTER', first_known_signup_date) AS first_known_signup_date_quarter
FROM first_rows
ORDER BY birth_year_avg_2_prior DESC, birth_year_share_of_country_plan
