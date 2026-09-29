WITH scoped AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown,
        SUBSTRING(TRIM(plan), 1, 2) AS plan_prefix
    FROM listeners
),
flattened AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.plan,
        s.country,
        s.signup_date,
        s.birth_year,
        s.country_or_unknown,
        s.plan_prefix,
        t.interest
    FROM scoped s
    LATERAL VIEW OUTER EXPLODE(s.interests) t AS interest
),
country_stats AS (
    -- Median birth_year per country
    SELECT country, PERCENTILE_APPROX(birth_year, 0.5) AS median_birth_year
    FROM flattened
    GROUP BY country
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_birth_year,
        ROUND(s.birth_year - g.median_birth_year, 2) AS birth_year_minus_median
    FROM flattened s
    JOIN country_stats g ON g.country = s.country
    WHERE s.birth_year >= g.median_birth_year
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(median_birth_year / NULLIF(SUM(median_birth_year) OVER (PARTITION BY country), 0), 4) AS median_birth_year_share_of_country,
        SUM(birth_year_minus_median) OVER (PARTITION BY country) AS country_sum_birth_year_minus_median
    FROM above_average
)
SELECT
    id,
    username,
    plan,
    country,
    signup_date,
    plan_prefix,
    interest,
    median_birth_year,
    birth_year_minus_median,
    CASE WHEN birth_year >= 1990 THEN 'HIGH' WHEN birth_year >= 1969 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier
FROM ranked
WHERE plan = 'premium'
ORDER BY birth_year_tier, median_birth_year DESC, id, interest
LIMIT 5
