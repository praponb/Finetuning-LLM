WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('escalated', 'assigned') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        DENSE_RANK() OVER w_event_type_group_actor_by_ev AS event_at_dense_rank,
        RANK() OVER w_event_type_group_actor_by_ev AS event_at_rank
    FROM prepared
    WINDOW w_event_type_group_actor_by_ev AS (PARTITION BY event_type_group, actor ORDER BY event_at DESC NULLS FIRST)
),
combined_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_at_rank,
        t.tags AS ticket_tags
    FROM with_trends s
    JOIN tickets t ON t.id = s.ticket_id
),
enriched AS (
    -- Join tickets
    SELECT
        s.*,
        t.tags AS ticket_tags_2,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id
    FROM combined_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id_2,
        t.agent_id AS ticket_agent_id
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id AND t.priority IN ('P1', 'P2')
    WHERE t.agent_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per ticket_priority
    SELECT
        id,
        ticket_id,
        actor,
        event_at_rank,
        ticket_tags,
        ticket_tags_2,
        ticket_priority,
        ticket_requester_id,
        ticket_requester_id_2,
        ticket_agent_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_priority ORDER BY event_at_rank DESC, id DESC) AS rn
        FROM with_parent
    ) ranked
    WHERE rn = 1
)
SELECT
    ticket_id,
    ticket_priority,
    GROUPING(ticket_id) AS is_total_ticket_id,
    MAX(ticket_requester_id) AS max_ticket_requester_id
FROM latest
GROUP BY ROLLUP(ticket_id, ticket_priority)
ORDER BY ticket_id NULLS LAST, ticket_priority NULLS LAST
