WITH valid AS (
    SELECT *, DATE_FORMAT(event_time, 'MMM yyyy') AS event_time_label
    FROM web_events
    WHERE page IS DISTINCT FROM '/home'
      AND event_type = 'page_view'
)
SELECT
    *,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
    AVG(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d,
    COUNT(duration_sec) OVER (PARTITION BY MONTH(event_time), page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_count_7d,
    NTILE(10) OVER (ORDER BY duration_sec DESC, id) AS duration_sec_bucket
FROM valid;
