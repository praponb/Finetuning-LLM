WITH in_window AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', priority, event_type) AS priority_event_type_label,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled
    FROM in_window
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id
    FROM annotated s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.priority_event_type_label,
        s.hours_since_open_filled,
        s.max_ticket_requester_id,
        t.priority AS ticket_priority,
        t.csat AS ticket_csat
    FROM profiled s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT id, metric, reading
FROM (
    SELECT id, hours_since_open_filled, hours_since_open
    FROM combined_details
) src
UNPIVOT (
    reading FOR metric IN (hours_since_open_filled, hours_since_open)
) unp
ORDER BY id, metric
