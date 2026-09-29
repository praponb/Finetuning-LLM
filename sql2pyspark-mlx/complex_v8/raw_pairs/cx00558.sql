WITH base AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATEDIFF(DATE '2026-01-01', signup_date) AS days_since_signup_date,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek
    FROM listeners
    WHERE UPPER(TRIM(username)) LIKE 'H%'
),
per_label AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.plan,
        s.country,
        s.signup_date,
        s.birth_year,
        s.days_since_signup_date,
        s.country_or_unknown,
        s.signup_date_dayofweek,
        t.interest_pos,
        t.interest
    FROM base s
    LATERAL VIEW POSEXPLODE(s.interests) t AS interest_pos, interest
),
tagged AS (
    -- Derive helper columns
    SELECT *, COALESCE(interest, 'UNKNOWN') AS interest_or_unknown
    FROM per_label
    WHERE birth_year < 1986
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        birth_year,
        country_or_unknown,
        signup_date_dayofweek,
        interest_pos,
        interest,
        SIZE(COLLECT_SET(country_or_unknown) OVER w_interest_pos_plan) AS distinct_country_or_unknown_in_interest_pos_plan,
        ROUND(interest_pos - AVG(interest_pos) OVER (PARTITION BY interest_pos, plan), 2) AS interest_pos_vs_interest_pos_plan_avg,
        MIN(days_since_signup_date) OVER w_interest_pos_plan AS interest_pos_plan_min_days_since_signup_date,
        LAG(days_since_signup_date) OVER (PARTITION BY country_or_unknown ORDER BY days_since_signup_date DESC, id, interest_pos) AS prev_days_since_signup_date,
        MAX(days_since_signup_date) OVER (PARTITION BY interest_pos, plan) AS interest_pos_plan_max_days_since_signup_date
    FROM tagged
    WINDOW w_interest_pos_plan AS (PARTITION BY interest_pos, plan)
),
with_trends_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(signup_date_dayofweek / NULLIF(SUM(signup_date_dayofweek) OVER w_interest_pos_signup_date_day, 0), 4) AS signup_date_dayofweek_share_of_interest_pos_sign,
        LEAD(birth_year, 1, 0) OVER (PARTITION BY distinct_country_or_unknown_in_interest_pos_plan ORDER BY distinct_country_or_unknown_in_interest_pos_plan DESC, id, interest_pos) AS next_birth_year,
        MAX(signup_date_dayofweek) OVER (PARTITION BY interest, interest_pos ORDER BY interest_pos_plan_min_days_since_signup_date ASC, id, interest_pos ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS signup_date_dayofweek_max_3_prior,
        ROUND(interest_pos - AVG(interest_pos) OVER (PARTITION BY interest_pos, signup_date_dayofweek), 2) AS interest_pos_vs_interest_pos_signup_date_dayofwe,
        SUM(prev_days_since_signup_date) OVER (ORDER BY birth_year DESC, id, interest_pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_days_since_signup_date
    FROM with_trends
    WINDOW w_interest_pos_signup_date_day AS (PARTITION BY interest_pos, signup_date_dayofweek)
)
-- Final projection
SELECT
    id,
    birth_year,
    country_or_unknown,
    signup_date_dayofweek,
    interest,
    distinct_country_or_unknown_in_interest_pos_plan,
    interest_pos_vs_interest_pos_plan_avg,
    prev_days_since_signup_date,
    interest_pos_plan_max_days_since_signup_date,
    signup_date_dayofweek_share_of_interest_pos_sign,
    next_birth_year,
    signup_date_dayofweek_max_3_prior,
    LENGTH(TRIM(interest)) AS interest_len
FROM with_trends_2
WHERE next_birth_year <= 1986
