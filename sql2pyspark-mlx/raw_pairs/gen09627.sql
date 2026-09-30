WITH filtered AS (
    SELECT
        *,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        DATEDIFF(CURRENT_DATE(), event_time) / 365.25 AS years_since_event_time
    FROM web_events
    WHERE event_time >= TO_TIMESTAMP('2023-04-30 17:00:00')
      AND HOUR(event_time) BETWEEN 6 AND 14
)
SELECT
    dim.device,
    src.event_type,
    COUNT(*) AS rows_total,
    SUM(src.duration_sec) AS total_duration_sec,
    ROUND(AVG(src.duration_sec_filled), 2) AS avg_duration_sec_filled
FROM filtered src
INNER JOIN sessions dim ON src.session_id = dim.id
GROUP BY GROUPING SETS ((dim.device, src.event_type), (dim.device))
ORDER BY rows_total NULLS FIRST
