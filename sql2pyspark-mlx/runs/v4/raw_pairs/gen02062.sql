WITH valid AS (
    SELECT
        id,
        session_id,
        event_time,
        duration_sec,
        UPPER(event_type) AS event_type_upper,
        TRIM(event_type) AS clean_event_type,
        COALESCE(page, 'OTHER') AS page_clean
    FROM web_events
    WHERE TO_DATE(event_time) >= '2025-02-28'
)
SELECT *
FROM valid
QUALIFY RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS FIRST) = 1
