WITH base AS (
    SELECT *, CONCAT(event_type, ', ', page) AS event_type_page
    FROM web_events
    WHERE page IS NULL
),
summary AS (
    SELECT DATE_TRUNC('MONTH', event_time) AS event_month, MAX(duration_sec) AS max_duration_sec
    FROM base
    GROUP BY DATE_TRUNC('MONTH', event_time)
)
SELECT event_month, max_duration_sec
FROM summary;
