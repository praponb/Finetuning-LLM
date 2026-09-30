WITH recent_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        CASE WHEN birth_year >= 1980 THEN 'HIGH' WHEN birth_year >= 1975 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier,
        CASE WHEN country IN ('JP', 'KR') THEN 'group_a' WHEN country = 'TH' THEN 'group_b' ELSE 'other' END AS country_group,
        ADD_MONTHS(signup_date, 1) AS signup_date_plus_1m
    FROM listeners
    WHERE birth_year < 1995
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        birth_year_tier,
        YEAR(signup_date_plus_1m) AS signup_date_plus_1m_year,
        plan,
        SUM(birth_year) AS total_birth_year,
        COUNT(DISTINCT id) AS distinct_ids,
        DATEDIFF(MAX(signup_date), MIN(signup_date)) AS signup_date_span_days,
        ROUND(100 * COUNT(CASE WHEN username IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_username_is_not_null,
        ROUND(AVG(birth_year), 2) AS avg_birth_year
    FROM recent_listeners
    GROUP BY birth_year_tier, YEAR(signup_date_plus_1m), plan
)
SELECT *
FROM (
    SELECT birth_year_tier, plan, signup_date_plus_1m_year
    FROM grouped
) src
PIVOT (
    AVG(signup_date_plus_1m_year)
    FOR plan IN ('student', 'premium', 'free')
)
ORDER BY birth_year_tier
