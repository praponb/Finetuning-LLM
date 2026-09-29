WITH cleaned AS (
    -- Filter tickets
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
    WHERE csat > 3
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.csat,
        s.tags,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM cleaned s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    csat,
    agent_id,
    COUNT(*) AS cnt,
    ARRAY_SORT(COLLECT_SET(event_actor)) AS event_actor_list
FROM with_children_events
GROUP BY GROUPING SETS ((csat, agent_id), (csat), (agent_id))
ORDER BY csat NULLS LAST, agent_id NULLS LAST
