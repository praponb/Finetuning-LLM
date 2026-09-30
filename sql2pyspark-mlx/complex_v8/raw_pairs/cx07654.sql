WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(csat) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_3_prior,
        LAG(closed_at) OVER (ORDER BY closed_at, id) AS prev_closed_at,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat,
        LAG(opened_at, 2) OVER (PARTITION BY channel ORDER BY closed_at DESC, id) AS prev2_opened_at,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        RANK() OVER w_all_by_csat_ties AS csat_rank,
        SUM(csat) OVER w_csat AS csat_sum_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY channel ORDER BY closed_at DESC), 3) AS closed_at_cume_dist
    FROM scoped
    WINDOW w_all_by_csat_ties AS (ORDER BY csat), w_csat AS (PARTITION BY csat)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        closed_at,
        priority,
        channel,
        tags,
        csat_avg_3_prior,
        distinct_agent_id_in_csat,
        prev2_opened_at,
        rows_in_agent_id,
        csat_rank,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        SIZE(COLLECT_SET(priority) OVER w_channel) AS distinct_priority_in_channel,
        LAG(prev2_opened_at, 3) OVER (PARTITION BY priority, csat ORDER BY prev2_opened_at, id) AS prev3_prev2_opened_at
    FROM windowed
    WINDOW w_channel AS (PARTITION BY channel)
)
-- Final projection
SELECT
    id,
    closed_at,
    priority,
    tags,
    csat_avg_3_prior,
    distinct_agent_id_in_csat,
    prev2_opened_at,
    csat_rank,
    distinct_agent_id_in_channel,
    prev3_prev2_opened_at
FROM with_windows
