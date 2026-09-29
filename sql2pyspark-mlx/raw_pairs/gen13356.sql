WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(page)) AS page_title,
        DATEDIFF(CURRENT_DATE(), event_time) / 7 AS weeks_since_event_time
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-05-20 20:00:00'
      AND page IS NOT NULL
)
SELECT
    *,
    SUM(duration_sec) OVER (PARTITION BY session_id) AS session_id_sum_duration_sec,
    SUM(weeks_since_event_time) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY weeks_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_weeks_since_event_time,
    LAG(weeks_since_event_time, 1) OVER (PARTITION BY event_type, session_id ORDER BY weeks_since_event_time, id) AS prev_weeks_since_event_time,
    MAX(weeks_since_event_time) OVER (PARTITION BY page) AS page_max_weeks_since_event_time,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
FROM base;
