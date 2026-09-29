WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('MONTH', event_at) AS event_at_month
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per event_type
    SELECT *, LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.burst_id,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM sessionized s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
tagged AS (
    -- Label and score rows
    SELECT *, DATE_FORMAT(event_event_at, 'yyyy-MM-dd') AS event_event_at_label
    FROM with_children
    WHERE burst_id >= 2
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    burst_id,
    event_id,
    event_actor,
    event_event_at,
    event_event_at_label,
    DATE_TRUNC('HOUR', event_event_at) AS event_event_at_hour,
    CONCAT(event_type, '#', CAST(event_id AS STRING)) AS event_type_ref
FROM tagged
ORDER BY burst_id, event_id DESC, id
LIMIT 10
