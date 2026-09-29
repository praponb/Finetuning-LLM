WITH prepared AS (
    -- Filter tickets
    SELECT *
    FROM tickets
),
joined_agent AS (
    -- Join support_agents
    SELECT s.*, su.team AS agent_team
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        ROUND(AVG(requester_id) OVER (ORDER BY opened_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_1_prior,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY channel), 2) AS requester_id_vs_channel_avg,
        LAST_VALUE(opened_at) OVER (PARTITION BY csat ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        LAST_VALUE(csat) OVER (PARTITION BY channel, csat ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_share_of_agent_id,
        MIN(csat) OVER (PARTITION BY agent_id) AS agent_id_min_csat,
        csat - LAG(csat) OVER (PARTITION BY priority, channel ORDER BY csat, id) AS csat_change,
        ROW_NUMBER() OVER w_all_by_csat AS csat_seq,
        FIRST_VALUE(csat) OVER (PARTITION BY csat, priority ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        ROUND(AVG(csat) OVER w_channel, 2) AS channel_avg_csat,
        RANK() OVER (ORDER BY csat ASC, id) AS csat_rank,
        DENSE_RANK() OVER (PARTITION BY csat, priority ORDER BY closed_at DESC, id) AS closed_at_dense_rank,
        SIZE(COLLECT_SET(priority) OVER w_agent_id) AS distinct_priority_in_agent_id,
        ROUND(PERCENT_RANK() OVER w_all_by_csat_ties, 3) AS csat_percent_rank,
        ROW_NUMBER() OVER w_channel_csat_by_closed_at AS closed_at_seq
    FROM joined_agent
    WINDOW w_all_by_csat AS (ORDER BY csat ASC, id), w_channel AS (PARTITION BY channel), w_agent_id AS (PARTITION BY agent_id), w_all_by_csat_ties AS (ORDER BY csat ASC), w_channel_csat_by_closed_at AS (PARTITION BY channel, csat ORDER BY closed_at, id)
),
first_rows AS (
    -- Top 5 per group
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
        agent_team,
        distinct_agent_id_in_channel,
        requester_id_avg_1_prior,
        requester_id_vs_channel_avg,
        last_opened_at,
        last_csat,
        requester_id_share_of_agent_id,
        agent_id_min_csat,
        csat_change,
        csat_seq,
        first_csat,
        channel_avg_csat,
        csat_rank,
        closed_at_dense_rank,
        distinct_priority_in_agent_id,
        csat_percent_rank
    FROM with_windows
    WHERE closed_at_seq <= 5
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    closed_at,
    channel,
    tags,
    agent_team,
    distinct_agent_id_in_channel,
    requester_id_avg_1_prior,
    last_opened_at,
    requester_id_share_of_agent_id,
    agent_id_min_csat,
    first_csat,
    channel_avg_csat,
    csat_rank,
    closed_at_dense_rank,
    csat_percent_rank,
    CASE WHEN channel IN ('web', 'phone') THEN 'group_a' WHEN channel = 'email' THEN 'group_b' ELSE 'other' END AS channel_group
FROM first_rows
