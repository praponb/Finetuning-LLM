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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        LENGTH(TRIM(priority)) AS priority_len
    FROM in_window
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM labeled s
)
-- Final projection
SELECT
    id,
    event_type,
    priority,
    hours_since_open,
    priority_or_unknown,
    event_type_ref,
    priority_len
FROM with_stats
ORDER BY priority NULLS LAST
