WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        HOUR(opened_at) AS opened_at_hour
    FROM tickets
    WHERE priority IN ('P4', 'P3')
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM base_tickets s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT *
FROM (
    SELECT agent_id, event_id, event_actor, csat
    FROM with_children_events
) src
PIVOT (
    MIN(csat) AS min_v, SUM(csat) AS sum_v
    FOR event_actor IN ('system', 'agent', 'customer')
)
