WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        CASE WHEN channel IN ('web', 'email') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 10 AND 13
      AND priority = 'P4'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY csat, channel_group ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY MONTH(opened_at), agent_id), 0), 4) AS csat_share_of_opened_at_month_agent_id,
        MIN(csat) OVER (PARTITION BY agent_id, csat) AS agent_id_csat_min_csat,
        ROUND(AVG(requester_id) OVER w_agent_id_csat, 2) AS agent_id_csat_avg_requester_id,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY MONTH(opened_at), agent_id), 0), 4) AS requester_id_share_of_opened_at_month_agent_id,
        COUNT(*) OVER (PARTITION BY MONTH(opened_at), agent_id) AS rows_in_opened_at_month_agent_id,
        NTILE(4) OVER (PARTITION BY channel_group ORDER BY csat ASC, id) AS csat_quartile,
        ROUND(csat - AVG(csat) OVER w_opened_at_month_agent_id, 2) AS csat_vs_opened_at_month_agent_id_avg,
        FIRST_VALUE(requester_id) OVER (PARTITION BY channel_group ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        ROUND(PERCENT_RANK() OVER w_csat_channel_group_by_opened_2, 3) AS opened_at_percent_rank
    FROM filtered
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat), w_opened_at_month_agent_id AS (PARTITION BY MONTH(opened_at), agent_id), w_csat_channel_group_by_opened_2 AS (PARTITION BY csat, channel_group ORDER BY opened_at NULLS LAST)
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    tags,
    second_opened_at,
    csat_share_of_opened_at_month_agent_id,
    agent_id_csat_min_csat,
    agent_id_csat_avg_requester_id,
    rows_in_agent_id_csat,
    csat_vs_opened_at_month_agent_id_avg,
    first_requester_id,
    opened_at_percent_rank,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
    CASE WHEN opened_at_percent_rank >= 0.8 THEN 'large' WHEN opened_at_percent_rank >= 0.5 THEN 'medium' ELSE 'small' END AS opened_at_percent_rank_tier
FROM sequenced
ORDER BY opened_at DESC, priority DESC NULLS FIRST
