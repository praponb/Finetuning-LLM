WITH matched_keys AS (
    SELECT event_type, session_id
    FROM web_events
    INTERSECT
    SELECT event_type, session_id
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 11 AND 15
),
reconciled AS (
    SELECT s.*
    FROM web_events s
    JOIN matched_keys k ON s.event_type = k.event_type AND s.session_id = k.session_id
)
SELECT
    session_id,
    event_type,
    COUNT(*) AS cnt,
    MIN(event_time) AS first_event_time,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    DENSE_RANK() OVER (PARTITION BY session_id ORDER BY COUNT(*) DESC) AS cnt_dense_rank
FROM reconciled
GROUP BY session_id, event_type
ORDER BY distinct_event_types
