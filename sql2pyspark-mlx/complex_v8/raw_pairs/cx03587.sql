WITH filtered_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_ADD(signup_date, 7) AS signup_date_plus_7d
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'jazz')
      AND plan IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        plan,
        country,
        interests,
        signup_date_plus_7d,
        COUNT(*) OVER (PARTITION BY country ORDER BY birth_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered_listeners
)
SELECT
    COALESCE(country, 'ALL') AS country,
    COALESCE(plan, 'ALL') AS plan,
    GROUPING(country) AS is_total_country,
    ROUND(AVG(running_count), 2) AS avg_running_count
FROM windowed
GROUP BY GROUPING SETS ((country, plan), (country), (plan))
ORDER BY country NULLS LAST, plan NULLS LAST
