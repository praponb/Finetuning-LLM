WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT session_id, event_type
    FROM web_events
    INTERSECT ALL
    SELECT session_id, event_type
    FROM web_events
    WHERE duration_sec != 301
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    JOIN remaining_keys k ON s.session_id = k.session_id AND s.event_type = k.event_type
)
SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec
FROM verified
ORDER BY event_time DESC, page ASC
