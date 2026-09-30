WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE ticket_id <= 13
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM prepared_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_len,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags
    FROM sessionized s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    ticket_id,
    COALESCE(event_type, 'ALL') AS event_type,
    event_type_len,
    COUNT(*) AS row_count,
    SUM(event_type_len) AS total_event_type_len
FROM enriched_ticket
GROUP BY CUBE(ticket_id, event_type, event_type_len)
ORDER BY ticket_id NULLS LAST, event_type NULLS LAST, event_type_len NULLS LAST
