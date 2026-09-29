WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('system', 'agent') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE actor NOT IN ('agent', 'customer')
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT id, metric_name, value
FROM (
    SELECT id, new_session_flag, burst_id
    FROM sessionized
) src
UNPIVOT (
    value FOR metric_name IN (new_session_flag AS m_new_session_flag, burst_id AS burst_id_v)
) unp
ORDER BY id, metric_name
