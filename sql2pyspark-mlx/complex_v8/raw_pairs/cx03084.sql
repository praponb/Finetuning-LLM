WITH cleaned AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests,
        CASE WHEN country IN ('BR', 'TH') THEN 'group_a' WHEN country = 'KR' THEN 'group_b' ELSE 'other' END AS country_group
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'indie')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY country_group, plan ORDER BY signup_date DESC), 3) AS signup_date_cume_dist,
        ROW_NUMBER() OVER w_country_group_plan_by_signup_2 AS signup_date_seq,
        ROUND(AVG(birth_year) OVER (PARTITION BY plan, country_group), 2) AS plan_country_group_avg_birth_year,
        COUNT(*) OVER w_country_group_plan AS rows_in_country_group_plan,
        SIZE(COLLECT_SET(country_group) OVER w_signup_date_month_plan) AS distinct_country_group_in_signup_date_month_plan,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER w_country_group_plan, 0), 4) AS birth_year_share_of_country_group_plan,
        LAG(signup_date, 2) OVER w_country_group_plan_by_signup_2 AS prev2_signup_date,
        LAG(birth_year, 3) OVER (PARTITION BY country_group, plan ORDER BY signup_date DESC, id) AS prev3_birth_year
    FROM cleaned
    WINDOW w_country_group_plan_by_signup_2 AS (PARTITION BY country_group, plan ORDER BY signup_date DESC, id), w_country_group_plan AS (PARTITION BY country_group, plan), w_signup_date_month_plan AS (PARTITION BY DATE_TRUNC('MONTH', signup_date), plan)
)
SELECT
    id,
    plan,
    signup_date,
    birth_year,
    interests,
    country_group,
    signup_date_seq,
    plan_country_group_avg_birth_year,
    rows_in_country_group_plan,
    prev2_signup_date,
    LENGTH(TRIM(username)) AS username_len,
    LENGTH(TRIM(plan)) AS plan_len
FROM sequenced
WHERE plan IS NOT NULL
ORDER BY rows_in_country_group_plan DESC NULLS FIRST
