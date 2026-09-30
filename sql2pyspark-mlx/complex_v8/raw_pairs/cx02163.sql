WITH valid AS (
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
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM tickets
    WHERE (TO_DATE(opened_at) < DATE '2025-01-31' OR channel NOT IN ('phone', 'email'))
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.minutes_opened_at_to_closed_at,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier = 'T1'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(requester_id) OVER (ORDER BY minutes_opened_at_to_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_priority_channel_by_requeste)) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(CUME_DIST() OVER w_agent_tier_channel_by_minute, 3) AS minutes_opened_at_to_closed_at_cume_dist,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_tier), 2) AS requester_id_vs_agent_tier_avg,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_tier, channel ORDER BY minutes_opened_at_to_closed_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_tier), 2) AS agent_tier_avg_requester_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_tier, channel ORDER BY minutes_opened_at_to_closed_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_3,
        LAG(requester_id) OVER (PARTITION BY agent_tier, channel ORDER BY minutes_opened_at_to_closed_at, id) AS prev_requester_id
    FROM with_parent
    WINDOW w_priority_channel_by_requeste AS (PARTITION BY priority, channel ORDER BY requester_id DESC, id), w_agent_tier_channel_by_minute AS (PARTITION BY agent_tier, channel ORDER BY minutes_opened_at_to_closed_at, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        opened_at,
        closed_at,
        priority,
        tags,
        minutes_opened_at_to_closed_at,
        agent_tier,
        first_requester_id,
        minutes_opened_at_to_closed_at_cume_dist,
        agent_tier_avg_requester_id,
        requester_id_avg_3,
        prev_requester_id,
        CASE WHEN priority IN ('P4', 'P3') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        MONTH(opened_at) AS opened_at_month
    FROM ranked
    WHERE channel = 'chat'
)
SELECT
    id,
    requester_id,
    opened_at,
    minutes_opened_at_to_closed_at,
    agent_tier,
    first_requester_id,
    minutes_opened_at_to_closed_at_cume_dist,
    requester_id_avg_3,
    priority_group,
    opened_at_month,
    ROUND(requester_id_avg_3 * 0.25, 2) AS requester_id_avg_3_adj,
    LEAST(requester_id, first_requester_id) AS least_requester_id_first_requester_id
FROM labeled
WHERE requester_id_avg_3 <= 20.2
ORDER BY opened_at DESC, opened_at_month, id
LIMIT 20
