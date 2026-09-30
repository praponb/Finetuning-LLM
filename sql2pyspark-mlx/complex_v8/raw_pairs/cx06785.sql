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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        MIN(hours_since_open) OVER (PARTITION BY ticket_id, priority ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_hours_since_open_so_far,
        COUNT(*) OVER (PARTITION BY ticket_id, agent_id) AS rows_in_ticket_id_agent_id,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, priority ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM joined_ranges
)
SELECT *
FROM (
    SELECT priority, event_at, event_type, min_hours_since_open_so_far
    FROM sequenced
) src
PIVOT (
    COUNT(min_hours_since_open_so_far) AS count_v, MAX(min_hours_since_open_so_far) AS max_v
    FOR event_type IN ('created', 'resolved', 'replied')
)
