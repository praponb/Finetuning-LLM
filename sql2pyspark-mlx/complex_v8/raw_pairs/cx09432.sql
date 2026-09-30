WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 60 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.prev_event_at,
        s.starts_session,
        s.burst_id,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM sessionized s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IN ('escalated', 'resolved', 'replied')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        starts_session,
        burst_id,
        event_id,
        event_event_at,
        CONCAT_WS(': ', event_type, event_event_type) AS event_type_event_event_type_label,
        DATEDIFF(DATE '2026-01-01', prev_event_at) AS days_since_prev_event_at,
        CONCAT_WS('-', event_type, event_event_type) AS event_type_event_event_type_label_2
    FROM line_level_events
    WHERE starts_session >= 0
)
SELECT
    id,
    ticket_id,
    starts_session,
    burst_id,
    event_event_at,
    event_type_event_event_type_label,
    days_since_prev_event_at,
    event_type_event_event_type_label_2,
    CASE WHEN event_type IN ('replied', 'created') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group
FROM enriched
ORDER BY ticket_id DESC
