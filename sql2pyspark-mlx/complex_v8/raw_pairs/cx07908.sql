WITH scoped AS (
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
    WHERE priority IS NOT NULL
      AND agent_id IS NOT NULL
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM scoped s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        agent_name,
        DENSE_RANK() OVER (PARTITION BY priority, agent_tier ORDER BY agent_hired_on DESC, id) AS agent_hired_on_dense_rank,
        LAST_VALUE(requester_id) IGNORE NULLS OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_requester_id,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id NULLS LAST, id) AS rn,
        DENSE_RANK() OVER w_priority_channel_by_agent_hi AS agent_hired_on_dense_rank_2,
        ROUND(CUME_DIST() OVER w_agent_id_by_requester_id_tie, 3) AS requester_id_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id, id), 3) AS requester_id_cume_dist_2,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY opened_at DESC, id) AS opened_at_decile,
        MIN(requester_id) OVER (PARTITION BY agent_tier, TO_DATE(closed_at)) AS agent_tier_closed_at_to_date_min_requester_id,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on,
        ROUND(AVG(requester_id) OVER (PARTITION BY priority, channel ORDER BY agent_hired_on, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_1_prior,
        requester_id - LAG(requester_id) OVER (ORDER BY requester_id, id) AS requester_id_change,
        MAX(requester_id) OVER (PARTITION BY priority ORDER BY agent_hired_on DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_max_3
    FROM with_parent
    WINDOW w_priority_channel_by_agent_hi AS (PARTITION BY priority, channel ORDER BY agent_hired_on, id), w_agent_id_by_requester_id_tie AS (PARTITION BY agent_id ORDER BY requester_id NULLS LAST)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        CASE WHEN requester_id_cume_dist >= 0.8 THEN 'large' WHEN requester_id_cume_dist >= 0.2 THEN 'medium' ELSE 'small' END AS requester_id_cume_dist_tier
    FROM sequenced
    WHERE priority IS NOT NULL
)
SELECT
    id,
    requester_id,
    closed_at,
    priority,
    agent_name,
    agent_hired_on_dense_rank,
    last_known_requester_id,
    rn,
    requester_id_cume_dist,
    requester_id_avg_1_prior,
    requester_id_change,
    requester_id_max_3,
    priority_group,
    requester_id_cume_dist_tier
FROM derived
