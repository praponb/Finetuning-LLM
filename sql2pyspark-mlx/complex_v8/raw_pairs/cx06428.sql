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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM in_window s
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        ticket_count,
        LEAD(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS next_event_at,
        LEAD(ticket_count, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at, id) AS next2_ticket_count,
        DENSE_RANK() OVER (PARTITION BY agent_id, event_type ORDER BY ticket_count ASC, id) AS ticket_count_dense_rank,
        ticket_count - LAG(ticket_count) OVER (PARTITION BY agent_id, event_type ORDER BY ticket_count ASC, id) AS ticket_count_change,
        NTILE(10) OVER (PARTITION BY agent_id, event_type ORDER BY ticket_count ASC, id) AS ticket_count_decile
    FROM summarized_rows
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM with_trends s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND NOT ARRAY_CONTAINS(t.tags, 'login')
        )
      AND s.agent_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    ticket_count,
    next2_ticket_count,
    ticket_count_dense_rank,
    ticket_count_change,
    DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
FROM with_activity
ORDER BY event_type DESC
