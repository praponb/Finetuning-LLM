WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags
    FROM tickets
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.tags,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM scoped_tickets s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.event_id,
        s.event_at,
        s.event_type,
        t.id AS event_id_2,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM with_children s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    event_id_2,
    event_actor,
    COUNT(*) FILTER (WHERE event_event_type = 'assigned') AS n_event_event_type_assigned,
    COUNT(DISTINCT event_actor) AS distinct_event_actors,
    COUNT(DISTINCT event_id) AS distinct_event_ids
FROM line_level_events
GROUP BY GROUPING SETS ((event_id_2, event_actor), (event_id_2), ())
