WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-02 10:00:00'
),
latest AS (
    -- Keep the earliest row per actor, actor_group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        actor_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, actor_group ORDER BY event_at, id) AS recency_rank
        FROM prepared_events
    ) ranked
    WHERE recency_rank = 1
),
with_parent_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_group,
        t.closed_at AS ticket_closed_at,
        su.agent_name
    FROM latest s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
SELECT
    id,
    event_type,
    actor_group,
    ticket_closed_at
FROM with_parent_ticket
