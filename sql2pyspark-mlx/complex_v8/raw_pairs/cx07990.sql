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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        t.channel AS ticket_channel,
        t.priority AS ticket_priority
    FROM covered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM joined_ticket s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
        )
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        agent_id,
        ticket_priority,
        ROW_NUMBER() OVER (PARTITION BY priority ORDER BY event_at, id) AS seq_no,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM screened
)
-- Final projection
SELECT
    id,
    event_at,
    agent_id,
    ticket_priority,
    seq_no,
    second_event_at,
    FLOOR(seq_no / 2) * 2 AS seq_no_bucket,
    FLOOR(seq_no / 2) * 2 AS seq_no_bucket_2
FROM with_windows
ORDER BY seq_no DESC
