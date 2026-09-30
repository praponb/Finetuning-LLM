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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
stats AS (
    -- Group-level totals
    SELECT
        event_type,
        ticket_id,
        agent_id,
        COUNT(DISTINCT priority) AS distinct_prioritys,
        COUNT(*) AS cnt
    FROM covered
    GROUP BY event_type, ticket_id, agent_id
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    event_type,
    ticket_id,
    agent_id,
    distinct_prioritys,
    cnt,
    cnt + distinct_prioritys AS cnt_plus_distinct_prioritys,
    ABS(distinct_prioritys - 4) AS distinct_prioritys_distance
FROM stats
WHERE event_type IN ('reopened', 'created')
ORDER BY distinct_prioritys DESC, event_type DESC
