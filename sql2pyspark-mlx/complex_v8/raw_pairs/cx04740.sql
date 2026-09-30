WITH covered AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        priority,
        agent_id,
        hours_since_open,
        SUM(hours_since_open) OVER w_priority_ticket_id AS priority_ticket_id_sum_hours_since_open,
        ROW_NUMBER() OVER w_priority_event_type_by_event AS event_at_seq
    FROM covered
    WINDOW w_priority_ticket_id AS (PARTITION BY priority, ticket_id), w_priority_event_type_by_event AS (PARTITION BY priority, event_type ORDER BY event_at DESC, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        priority,
        agent_id,
        hours_since_open,
        priority_ticket_id_sum_hours_since_open,
        event_at_seq,
        LAG(event_at, 2) OVER w_event_at_seq_by_event_at_seq AS prev2_event_at,
        RANK() OVER (PARTITION BY event_at_seq ORDER BY event_at_seq) AS event_at_seq_rank
    FROM with_trends
    WINDOW w_event_at_seq_by_event_at_seq AS (PARTITION BY event_at_seq ORDER BY event_at_seq, id)
)
-- Final projection
SELECT
    id,
    event_at,
    priority,
    event_at_seq,
    prev2_event_at,
    event_at_seq_rank,
    SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
FROM with_windows
ORDER BY id DESC
LIMIT 10
