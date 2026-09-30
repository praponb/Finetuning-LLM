WITH prev_events AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    DATE_TRUNC('YEAR', prev_started_at) AS year_start,
    CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END AS new_group_flag_tier,
    SUM(CASE WHEN country = 'DE' THEN new_group_flag ELSE 0 END) AS de_new_group_flag,
    MIN(prev_started_at) AS first_prev_started_at,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
FROM islands
GROUP BY CUBE (DATE_TRUNC('YEAR', prev_started_at), CASE WHEN new_group_flag >= 1 THEN 'LARGE' ELSE 'SMALL' END)
HAVING de_new_group_flag > 0;
