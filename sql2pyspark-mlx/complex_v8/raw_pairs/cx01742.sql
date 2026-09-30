WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        COALESCE(requester_id, 0) AS requester_id_filled
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'refund')
      AND channel = 'web'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(opened_at) OVER (ORDER BY opened_at NULLS LAST, id) AS next_opened_at,
        COUNT(*) OVER (PARTITION BY priority, csat) AS rows_in_priority_csat,
        ROUND(CUME_DIST() OVER w_priority_agent_id_by_csat_ti, 3) AS csat_cume_dist,
        SUM(requester_id_filled) OVER w_closed_at_month_agent_id AS closed_at_month_agent_id_sum_requester_id_filled,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS closed_at_dense_rank,
        ROUND(CUME_DIST() OVER w_agent_id_by_closed_at_ties, 3) AS closed_at_cume_dist,
        ROW_NUMBER() OVER w_all_by_opened_at AS rn,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY MONTH(closed_at), agent_id), 0), 4) AS requester_id_share_of_closed_at_month_agent_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id)) AS distinct_priority_in_agent_id
    FROM scoped
    WINDOW w_priority_csat AS (PARTITION BY priority, csat), w_priority_agent_id_by_csat_ti AS (PARTITION BY priority, agent_id ORDER BY csat DESC), w_closed_at_month_agent_id AS (PARTITION BY MONTH(closed_at), agent_id), w_agent_id_by_closed_at_ties AS (PARTITION BY agent_id ORDER BY closed_at), w_all_by_opened_at AS (ORDER BY opened_at NULLS LAST, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        requester_id_filled,
        next_opened_at,
        rows_in_priority_csat,
        csat_cume_dist,
        closed_at_month_agent_id_sum_requester_id_filled,
        closed_at_cume_dist,
        rn,
        requester_id_share_of_closed_at_month_agent_id,
        distinct_priority_in_agent_id
    FROM ranked
    WHERE closed_at_dense_rank <= 5
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(requester_id_filled) OVER (ORDER BY closed_at_month_agent_id_sum_requester_id_filled DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_filled_avg_6_prior,
        ROUND(csat / NULLIF(SUM(csat) OVER w_rn_priority, 0), 4) AS csat_share_of_rn_priority,
        MAX(rows_in_priority_csat) OVER (PARTITION BY rn, priority) AS rn_priority_max_rows_in_priority_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY rn, priority)) AS distinct_agent_id_in_rn_priority,
        ROW_NUMBER() OVER (ORDER BY closed_at_month_agent_id_sum_requester_id_filled DESC, id) AS rn_2
    FROM top_per_group
    WINDOW w_rn_priority AS (PARTITION BY rn, priority)
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    requester_id_filled,
    next_opened_at,
    rows_in_priority_csat,
    csat_cume_dist,
    closed_at_month_agent_id_sum_requester_id_filled,
    closed_at_cume_dist,
    rn,
    requester_id_share_of_closed_at_month_agent_id,
    distinct_priority_in_agent_id,
    requester_id_filled_avg_6_prior,
    distinct_agent_id_in_rn_priority,
    rn_2,
    ABS(csat - 4) AS csat_distance
FROM sequenced
ORDER BY rows_in_priority_csat DESC, opened_at DESC, id
LIMIT 5
