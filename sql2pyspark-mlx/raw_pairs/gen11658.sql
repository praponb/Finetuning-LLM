WITH filtered AS (
    SELECT
        *,
        TRIM(page) AS clean_page,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-10-17 14:00:00'
)
SELECT
    id,
    page,
    event_time,
    RANK() OVER (ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_rank,
    NTILE(10) OVER (PARTITION BY TO_DATE(event_time), event_type ORDER BY duration_sec, id) AS duration_sec_bucket,
    AVG(duration_sec) OVER (PARTITION BY event_type, page) AS event_type_avg_duration_sec,
    SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('month', event_time), session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
    SUM(duration_sec) OVER (PARTITION BY YEAR(event_time), event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_2
FROM filtered;
