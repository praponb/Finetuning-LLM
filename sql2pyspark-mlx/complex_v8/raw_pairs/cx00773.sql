WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-25 11:00:00'
      AND MONTH(closed_at) IN (2, 3)
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        su.tier AS agent_tier
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY opened_at DESC NULLS FIRST) AS opened_at_rank,
        DENSE_RANK() OVER (ORDER BY opened_at DESC NULLS FIRST) AS opened_at_dense_rank,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_tier, agent_id)) AS distinct_priority_in_agent_tier_agent_id,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id) AS requester_id_dense_rank,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        FIRST_VALUE(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        ROUND(AVG(requester_id) OVER w_agent_tier, 2) AS agent_tier_avg_requester_id,
        ROUND(AVG(requester_id) OVER (ORDER BY closed_at NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_6_prior,
        SUM(requester_id) OVER (PARTITION BY agent_tier) AS agent_tier_sum_requester_id,
        SUM(requester_id) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        SUM(requester_id) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_2,
        MIN(requester_id) OVER (PARTITION BY agent_tier) AS agent_tier_min_requester_id
    FROM with_parent_agent
    WINDOW w_agent_tier_agent_id AS (PARTITION BY agent_tier, agent_id), w_agent_tier AS (PARTITION BY agent_tier)
)
-- Final projection
SELECT
    id,
    agent_id,
    agent_tier,
    opened_at_rank,
    opened_at_dense_rank,
    distinct_priority_in_agent_tier_agent_id,
    requester_id_dense_rank,
    requester_id_avg_3,
    first_closed_at,
    agent_tier_avg_requester_id,
    requester_id_avg_6_prior,
    agent_tier_sum_requester_id,
    running_requester_id,
    running_requester_id_2,
    COALESCE(agent_tier, 'UNKNOWN') AS agent_tier_or_unknown
FROM metrics
WHERE agent_id >= 6
ORDER BY first_closed_at DESC, agent_tier_sum_requester_id DESC
