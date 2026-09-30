WITH recent_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        CONCAT_WS('|', country, plan) AS country_plan_label,
        CASE WHEN birth_year >= 1986 THEN 'A' WHEN birth_year >= 1975 THEN 'B' ELSE 'C' END AS birth_year_tier
    FROM listeners
    WHERE signup_date < DATE '2025-01-29'
      AND country IS NOT NULL
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.plan,
        s.country,
        s.signup_date,
        s.birth_year,
        s.country_plan_label,
        s.birth_year_tier,
        t.interest
    FROM recent_listeners s
    LATERAL VIEW EXPLODE(s.interests) t AS interest
)
SELECT
    birth_year_tier,
    plan,
    country,
    MAX(birth_year) AS max_birth_year,
    COUNT(DISTINCT interest) AS distinct_interests,
    ROUND(STDDEV(birth_year), 2) AS stddev_birth_year
FROM exploded
GROUP BY CUBE(birth_year_tier, plan, country)
