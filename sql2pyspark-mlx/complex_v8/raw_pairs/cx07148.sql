WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        CASE WHEN channel IN ('web', 'email') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 2)
      AND csat <= 3
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel_group,
        su.tier AS agent_tier
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier NOT IN ('T1', 'T3')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_agent_id_agent_tier_by_close AS closed_at_dense_rank,
        LAG(closed_at) OVER (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC, id) AS prev_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel_group, TO_DATE(closed_at) ORDER BY closed_at), 3) AS closed_at_percent_rank,
        LEAD(closed_at, 2) OVER w_channel_group_closed_at_to_d_2 AS next2_closed_at,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id, channel_group ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY channel_group, TO_DATE(closed_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        NTILE(4) OVER (ORDER BY closed_at DESC, id) AS closed_at_quartile,
        FIRST_VALUE(closed_at) OVER (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        DENSE_RANK() OVER (PARTITION BY channel_group, TO_DATE(closed_at) ORDER BY closed_at) AS closed_at_dense_rank_3,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROW_NUMBER() OVER (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC, id) AS closed_at_seq
    FROM with_parent
    WINDOW w_agent_id_agent_tier_by_close AS (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC), w_channel_group_closed_at_to_d_2 AS (PARTITION BY channel_group, TO_DATE(closed_at) ORDER BY closed_at, id)
)
SELECT
    id,
    closed_at,
    channel_group,
    closed_at_dense_rank,
    prev_closed_at,
    closed_at_percent_rank,
    first_known_closed_at,
    closed_at_dense_rank_3,
    closed_at_seq
FROM with_windows
