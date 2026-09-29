WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT page
    FROM web_events
    INTERSECT ALL
    SELECT page
    FROM web_events
    WHERE event_time < '2024-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.page = k.page
)
SELECT
    s.session_id,
    s.page,
    s.event_time,
    (SELECT SUM(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS sum_web_event_duration_sec
FROM reconciled s
ORDER BY session_id DESC NULLS FIRST
