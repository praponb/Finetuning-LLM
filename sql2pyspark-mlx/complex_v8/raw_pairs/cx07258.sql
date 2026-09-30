WITH prepared AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE NOT ARRAY_CONTAINS(interests, 'jazz')
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(birth_year - 1980) AS birth_year_distance,
        LENGTH(TRIM(username)) AS username_len,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM prepared
)
-- Subtotals with ROLLUP
SELECT
    plan,
    country,
    country_or_unknown,
    COUNT(*) FILTER (WHERE plan IS NOT NULL) AS n_plan_is_not_null,
    SUM(CASE WHEN country IN ('TH', 'KR', 'BR') THEN 1 ELSE 0 END) AS n_country_in_th_kr_br,
    MAX(signup_date) AS last_signup_date
FROM tagged
GROUP BY ROLLUP(plan, country, country_or_unknown)
ORDER BY plan NULLS LAST, country NULLS LAST, country_or_unknown NULLS LAST
