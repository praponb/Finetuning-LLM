WITH valid AS (
    SELECT
        *,
        INITCAP(TRIM(page)) page_title,
        FLOOR(duration_sec) duration_sec_whole
    FROM web_events
    WHERE page IS NOT NULL
      AND page IS DISTINCT FROM '/search'
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid
),
sessionized AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        page_title,
        duration_sec_whole,
        SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    CASE WHEN duration_sec_whole >= 90 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_whole_tier,
    MIN(event_time) AS first_event_time,
    AVG(burst_id) FILTER (WHERE page = '/home') AS home_avg_burst_id
FROM sessionized
GROUP BY duration_sec_whole_tier
