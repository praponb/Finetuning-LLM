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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.channel = 'web') FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_channel_count,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM joined_ranges s
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ABS(hours_since_open - 28.8) AS hours_since_open_distance,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM summarized_rows
)
SELECT
    id,
    event_at,
    event_type,
    priority,
    agent_id,
    ticket_channel_count,
    last_ticket_closed_at,
    event_type_ref,
    CONCAT(priority, '#', CAST(ticket_channel_count AS STRING)) AS priority_ref
FROM tagged
ORDER BY agent_id DESC
