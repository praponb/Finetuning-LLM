WITH recent AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        country,
        signup_date,
        interests,
        CONCAT_WS('-', country, plan) AS country_plan_label,
        DATEDIFF(DATE '2025-06-30', signup_date) AS days_since_signup_date,
        ABS(birth_year - 1980) AS birth_year_distance
    FROM listeners
    WHERE plan <> 'free'
      AND ARRAY_CONTAINS(interests, 'indie')
),
latest_per_key AS (
    -- Keep the earliest row per country
    SELECT
        id,
        plan,
        country,
        signup_date,
        interests,
        country_plan_label,
        days_since_signup_date,
        birth_year_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY days_since_signup_date, id) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
),
country_stats AS (
    -- Max birth_year_distance per country
    SELECT country, MAX(birth_year_distance) AS max_birth_year_distance
    FROM latest_per_key
    GROUP BY country
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_birth_year_distance,
        ROUND(s.birth_year_distance - g.max_birth_year_distance, 2) AS birth_year_distance_minus_max
    FROM latest_per_key s
    JOIN country_stats g ON g.country = s.country
    WHERE s.birth_year_distance = g.max_birth_year_distance
),
scored AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(country_plan_label)) AS country_plan_label_len
    FROM compared
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(days_since_signup_date, 2) OVER (PARTITION BY country, YEAR(signup_date) ORDER BY birth_year_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_signup_date,
        SIZE(COLLECT_SET(country) OVER w_plan) AS distinct_country_in_plan
    FROM scored
    WINDOW w_plan AS (PARTITION BY plan)
),
latest_per_key_2 AS (
    -- Keep the latest row per country
    SELECT
        id,
        plan,
        country,
        signup_date,
        interests,
        country_plan_label,
        days_since_signup_date,
        birth_year_distance,
        max_birth_year_distance,
        birth_year_distance_minus_max,
        country_plan_label_len,
        second_days_since_signup_date,
        distinct_country_in_plan
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY second_days_since_signup_date DESC, id DESC) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT country_plan_label_len, plan, birth_year_distance
    FROM latest_per_key_2
) src
PIVOT (
    SUM(birth_year_distance)
    FOR plan IN ('free', 'family', 'student', 'premium')
)
