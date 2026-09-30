WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
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
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        CASE WHEN channel IN ('email', 'phone') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd HH:00') AS closed_at_label
    FROM tickets
    WHERE (channel = 'phone' OR opened_at >= TIMESTAMP '2025-02-06 12:00:00')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        priority_group,
        channel_group,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority_group, TO_DATE(opened_at) ORDER BY opened_at DESC, id), 3) AS opened_at_percent_rank,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY csat, priority_group), 0), 4) AS csat_share_of_csat_priority_group,
        ROW_NUMBER() OVER w_priority_group_opened_at_to AS rn
    FROM cleaned_tickets
    WINDOW w_priority_group_opened_at_to AS (PARTITION BY priority_group, TO_DATE(opened_at) ORDER BY opened_at DESC, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        tags,
        channel_group,
        opened_at_percent_rank,
        csat_share_of_csat_priority_group,
        rn,
        NTILE(10) OVER (PARTITION BY priority_group, channel ORDER BY closed_at NULLS LAST, id) AS closed_at_decile,
        MIN(rn) OVER (PARTITION BY priority_group, channel ORDER BY closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rn_so_far,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg,
        LAG(opened_at_percent_rank, 2) OVER w_priority_group_channel_by_cl AS prev2_opened_at_percent_rank
    FROM with_windows
    WINDOW w_priority_group_channel_by_cl AS (PARTITION BY priority_group, channel ORDER BY closed_at NULLS LAST, id)
)
-- Subtotals with ROLLUP
SELECT
    rn,
    closed_at_decile,
    GROUPING_ID() AS grouping_level,
    MAX(closed_at_decile) AS max_closed_at_decile,
    COUNT(*) AS row_count,
    COUNT(DISTINCT agent_id) AS distinct_agent_ids
FROM with_trends
GROUP BY ROLLUP(rn, closed_at_decile)
