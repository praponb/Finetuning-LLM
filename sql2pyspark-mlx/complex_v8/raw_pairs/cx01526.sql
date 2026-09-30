WITH cleaned_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        COALESCE(birth_year, 0) AS birth_year_filled
    FROM listeners
    WHERE (country = 'JP' OR ARRAY_CONTAINS(interests, 'indie'))
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(birth_year_filled) OVER (PARTITION BY plan, country ORDER BY birth_year_filled, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS birth_year_filled_max_3,
        ROW_NUMBER() OVER (ORDER BY birth_year_filled ASC, id) AS rn,
        ROUND(AVG(birth_year_filled) OVER (ORDER BY birth_year_filled ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS birth_year_filled_avg_1_prior,
        ROUND(AVG(birth_year_filled) OVER (PARTITION BY plan, country ORDER BY birth_year_filled, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS birth_year_filled_avg_1_prior_2,
        birth_year_filled - LAG(birth_year_filled) OVER w_plan_country_by_birth_year_f_3 AS birth_year_filled_change,
        LAST_VALUE(signup_date) IGNORE NULLS OVER (PARTITION BY plan, country ORDER BY birth_year_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_signup_date,
        NTILE(3) OVER (ORDER BY birth_year_filled DESC NULLS FIRST, id) AS birth_year_filled_tercile,
        ROUND(AVG(birth_year_filled) OVER (PARTITION BY plan), 2) AS plan_avg_birth_year_filled,
        SIZE(COLLECT_SET(country) OVER (PARTITION BY plan)) AS distinct_country_in_plan,
        ROUND(birth_year_filled / NULLIF(SUM(birth_year_filled) OVER (PARTITION BY plan), 0), 4) AS birth_year_filled_share_of_plan
    FROM cleaned_listeners
    WINDOW w_plan_country_by_birth_year_f_3 AS (PARTITION BY plan, country ORDER BY birth_year_filled, id)
)
SELECT
    id,
    plan,
    country,
    signup_date,
    interests,
    birth_year_filled,
    birth_year_filled_max_3,
    rn,
    birth_year_filled_avg_1_prior,
    birth_year_filled_avg_1_prior_2,
    birth_year_filled_change,
    birth_year_filled_tercile,
    plan_avg_birth_year_filled,
    distinct_country_in_plan,
    DATE_TRUNC('MONTH', last_known_signup_date) AS last_known_signup_date_month,
    DATEDIFF(signup_date, last_known_signup_date) AS days_last_known_signup_date_to_signup_date
FROM sequenced
ORDER BY birth_year_filled_change, rn, id
LIMIT 20
