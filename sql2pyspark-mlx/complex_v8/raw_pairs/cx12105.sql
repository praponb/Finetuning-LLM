WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority,
        t.agent_id AS ticket_agent_id,
        su.team AS agent_team
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_priority,
        ticket_agent_id,
        agent_team,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(4) OVER w_all_by_event_at AS event_at_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_agent_id, ticket_id ORDER BY event_at), 3) AS event_at_cume_dist,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank,
        RANK() OVER (ORDER BY event_at, id) AS event_at_rank,
        NTILE(4) OVER (PARTITION BY actor, event_type ORDER BY event_at DESC, id) AS event_at_quartile_2,
        LEAD(event_at) OVER w_actor_event_type_by_event_at_2 AS next_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_agent_id, ticket_id ORDER BY event_at, id) AS prev_event_at,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at_2,
        LAG(event_at, 2) OVER w_all_by_event_at AS prev2_event_at
    FROM combined_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id), w_all_by_event_at_ties AS (ORDER BY event_at), w_actor_event_type_by_event_at_2 AS (PARTITION BY actor, event_type ORDER BY event_at DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_agent_id,
    event_at_quartile,
    event_at_dense_rank,
    event_at_rank,
    event_at_quartile_2,
    next_event_at,
    prev_event_at,
    prev_event_at_2,
    prev2_event_at,
    DATE_TRUNC('WEEK', second_event_at) AS second_event_at_week
FROM with_windows
ORDER BY ticket_agent_id, id
LIMIT 5
