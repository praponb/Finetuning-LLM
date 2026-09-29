WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE channel IN ('web', 'email')
      AND closed_at >= TIMESTAMP '2025-01-16 00:00:00'
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        t.id AS event_id,
        t.event_type,
        t.actor AS event_actor
    FROM cleaned s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT *
FROM (
    SELECT closed_at, event_id, priority, csat
    FROM exploded_detail_events
) src
PIVOT (
    SUM(csat)
    FOR priority IN ('P1', 'P4')
)
ORDER BY closed_at, event_id
