WITH joined_ranges AS (
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
        CONCAT_WS(': ', priority, event_type) AS priority_event_type_label,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled
    FROM joined_ranges
    WHERE agent_id >= 9
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(closed_at) AS last_ticket_closed_at,
        MIN(requester_id) AS min_ticket_requester_id,
        MIN(closed_at) AS first_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.last_ticket_closed_at,
        t.min_ticket_requester_id,
        t.first_ticket_closed_at
    FROM annotated s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    agent_id,
    hours_since_open,
    days_since_event_at,
    hours_since_open_filled,
    last_ticket_closed_at,
    first_ticket_closed_at,
    ROUND(hours_since_open / NULLIF(min_ticket_requester_id, 0), 3) AS hours_since_open_per_min_ticket_requester_id,
    DATE_TRUNC('DAY', last_ticket_closed_at) AS last_ticket_closed_at_day
FROM profiled
ORDER BY id
