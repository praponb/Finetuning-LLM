WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor IN ('agent', 'system') OR ticket_id <= 45)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        DENSE_RANK() OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at DESC, id) AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at DESC, id) AS next_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY event_at NULLS LAST, id), 3) AS event_at_percent_rank_2
    FROM base_events
),
joined AS (
    -- Join tickets
    SELECT
        s.*,
        t.tags AS ticket_tags,
        t.priority AS ticket_priority,
        su.agent_name
    FROM windowed s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.priority = 'P3'
),
enriched AS (
    -- Add calculated columns
    SELECT *, ABS(event_at_dense_rank - 7) AS event_at_dense_rank_distance
    FROM joined
    WHERE event_at_dense_rank <= 9
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE event_at_dense_rank_distance > (SELECT AVG(event_at_dense_rank_distance) FROM enriched)
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND ARRAY_CONTAINS(t.tags, 'refund')
        )
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_percent_rank,
    next_event_at,
    second_event_at_2,
    event_at_percent_rank_2,
    ticket_tags,
    agent_name,
    event_at_dense_rank_distance
FROM with_activity
WHERE next_event_at >= TIMESTAMP '2025-01-25 10:00:00'
ORDER BY next_event_at DESC
