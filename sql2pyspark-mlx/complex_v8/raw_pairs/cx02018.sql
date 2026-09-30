WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 23
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, t.agent_id AS ticket_agent_id, su.hired_on AS agent_hired_on
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        DENSE_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at NULLS LAST, id) AS event_at_dense_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, ticket_id ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTH_VALUE(agent_hired_on, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on_2,
        LAG(event_at, 3) OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC, id) AS prev3_event_at,
        LAST_VALUE(agent_hired_on) OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        DENSE_RANK() OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC) AS event_at_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at DESC), 3) AS event_at_percent_rank_2,
        LAG(agent_hired_on, 2) OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC, id) AS prev2_agent_hired_on
    FROM combined_details
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE event_at_dense_rank_2 <= 2
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(ticket_agent_id) OVER (PARTITION BY actor, MONTH(event_at))) AS distinct_ticket_agent_id_in_actor_event_at_month,
        MAX(event_at_dense_rank) OVER (ORDER BY event_at_dense_rank DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_at_dense_rank_max_3_prior,
        RANK() OVER w_ticket_id_event_at_dense_ran AS agent_hired_on_rank,
        ROUND(event_at_percent_rank - AVG(event_at_percent_rank) OVER (PARTITION BY actor, MONTH(event_at)), 2) AS event_at_percent_rank_vs_actor_event_at_month_av,
        MAX(event_at_dense_rank_2) OVER (PARTITION BY ticket_id, event_at_dense_rank ORDER BY agent_hired_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_at_dense_rank_2_max_4,
        LEAD(prev2_agent_hired_on) OVER (PARTITION BY ticket_id, event_at_dense_rank ORDER BY agent_hired_on, id) AS next_prev2_agent_hired_on
    FROM leaders
    WINDOW w_ticket_id_event_at_dense_ran AS (PARTITION BY ticket_id, event_at_dense_rank ORDER BY agent_hired_on)
)
SELECT
    id,
    event_type,
    agent_hired_on,
    event_at_percent_rank,
    second_agent_hired_on,
    event_at_dense_rank,
    first_event_at,
    second_agent_hired_on_2,
    prev3_event_at,
    event_at_dense_rank_2,
    event_at_percent_rank_2,
    distinct_ticket_agent_id_in_actor_event_at_month,
    event_at_dense_rank_max_3_prior,
    event_at_percent_rank_vs_actor_event_at_month_av,
    event_at_dense_rank_2_max_4,
    next_prev2_agent_hired_on,
    CASE WHEN event_at_dense_rank >= 6 THEN 'HIGH' WHEN event_at_dense_rank >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS event_at_dense_rank_tier
FROM metrics
ORDER BY next_prev2_agent_hired_on, agent_hired_on DESC NULLS FIRST
