WITH valid_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_date,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id > 7
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        event_at_date,
        ticket_priority,
        agent_hired_on,
        COUNT(*) OVER (PARTITION BY actor) AS rows_in_actor,
        SIZE(COLLECT_SET(ticket_priority) OVER (PARTITION BY actor)) AS distinct_ticket_priority_in_actor,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (ORDER BY ticket_requester_id ASC, id)) AS days_since_prev_agent_hired_on,
        DENSE_RANK() OVER (ORDER BY event_at, id) AS event_at_dense_rank,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ticket_requester_id_avg_3_prior,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_actor, 2) AS ticket_requester_id_vs_actor_avg,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY actor), 2) AS actor_avg_ticket_requester_id
    FROM enriched_ticket
    WINDOW w_actor AS (PARTITION BY actor)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        event_at,
        event_type,
        event_at_date,
        ticket_priority,
        agent_hired_on,
        rows_in_actor,
        distinct_ticket_priority_in_actor,
        days_since_prev_agent_hired_on,
        last_known_event_at,
        ticket_requester_id_avg_3_prior,
        ticket_requester_id_vs_actor_avg,
        actor_avg_ticket_requester_id
    FROM metrics
    WHERE event_at_dense_rank <= 2
)
-- Final projection
SELECT
    id,
    event_at,
    event_at_date,
    agent_hired_on,
    rows_in_actor,
    distinct_ticket_priority_in_actor,
    days_since_prev_agent_hired_on,
    last_known_event_at,
    ticket_requester_id_avg_3_prior,
    ticket_requester_id_vs_actor_avg,
    actor_avg_ticket_requester_id,
    ABS(rows_in_actor - 13) AS rows_in_actor_distance
FROM leaders
ORDER BY rows_in_actor
