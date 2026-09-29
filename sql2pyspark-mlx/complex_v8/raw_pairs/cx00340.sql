WITH filtered_listeners AS (
    -- Filter listeners
    SELECT
        id,
        plan,
        country,
        birth_year,
        DATE_TRUNC('QUARTER', signup_date) AS signup_date_quarter
    FROM listeners
    WHERE ARRAY_CONTAINS(interests, 'jazz')
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at
    FROM filtered_listeners s
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.id
              AND MONTH(p.played_at) IN (1, 3)
        )
)
-- Subtotals with CUBE
SELECT
    play_count,
    COALESCE(country, 'ALL') AS country,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT country) AS distinct_countrys
FROM with_activity
GROUP BY CUBE(play_count, country)
ORDER BY play_count NULLS LAST, country NULLS LAST
