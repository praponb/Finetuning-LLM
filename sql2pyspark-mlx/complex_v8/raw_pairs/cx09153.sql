WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'customer'
      AND event_type NOT IN ('resolved', 'escalated')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        RANK() OVER w_actor_event_at_month_by_even AS event_at_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC), 3) AS event_at_percent_rank
    FROM valid_events
    WINDOW w_actor_event_at_month_by_even AS (PARTITION BY actor, MONTH(event_at) ORDER BY event_at ASC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        CASE WHEN event_at_percent_rank >= 0.7 THEN 'large' WHEN event_at_percent_rank >= 0.3 THEN 'medium' ELSE 'small' END AS event_at_percent_rank_tier,
        actor,
        COUNT(*) FILTER (WHERE actor = 'agent') AS n_actor_agent,
        SUM(event_at_percent_rank) FILTER (WHERE actor = 'system') AS event_at_percent_rank_actor_system
    FROM with_windows
    GROUP BY CASE WHEN event_at_percent_rank >= 0.7 THEN 'large' WHEN event_at_percent_rank >= 0.3 THEN 'medium' ELSE 'small' END, actor
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(n_actor_agent) OVER (PARTITION BY event_at_percent_rank_tier ORDER BY event_at_percent_rank_actor_system DESC NULLS FIRST, event_at_percent_rank_tier, actor ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS n_actor_agent_max_1_prior,
        NTH_VALUE(n_actor_agent, 2) OVER (PARTITION BY event_at_percent_rank_tier ORDER BY event_at_percent_rank_actor_system DESC NULLS FIRST, event_at_percent_rank_tier, actor ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_actor_agent,
        ROUND(CUME_DIST() OVER w_actor_event_at_percent_rank, 3) AS n_actor_agent_cume_dist,
        ROUND(AVG(n_actor_agent) OVER w_event_at_percent_rank_tier, 2) AS event_at_percent_rank_tier_avg_n_actor_agent,
        SUM(n_actor_agent) OVER (PARTITION BY actor, event_at_percent_rank_tier ORDER BY n_actor_agent, event_at_percent_rank_tier, actor ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS n_actor_agent_sum_2_prior,
        DENSE_RANK() OVER (PARTITION BY event_at_percent_rank_tier ORDER BY event_at_percent_rank_actor_system DESC NULLS FIRST) AS event_at_percent_rank_actor_system_dense_rank
    FROM summary
    WINDOW w_actor_event_at_percent_rank AS (PARTITION BY actor, event_at_percent_rank_tier ORDER BY n_actor_agent), w_event_at_percent_rank_tier AS (PARTITION BY event_at_percent_rank_tier)
)
SELECT
    event_at_percent_rank_tier,
    event_at_percent_rank_actor_system,
    second_n_actor_agent,
    n_actor_agent_cume_dist,
    event_at_percent_rank_tier_avg_n_actor_agent,
    n_actor_agent_sum_2_prior,
    event_at_percent_rank_actor_system_dense_rank,
    ROUND(second_n_actor_agent / NULLIF(n_actor_agent, 0), 3) AS second_n_actor_agent_per_n_actor_agent,
    COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
FROM with_trends
WHERE n_actor_agent_cume_dist < 0.7
