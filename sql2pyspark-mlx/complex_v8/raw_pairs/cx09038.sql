WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        COALESCE(csat, 0) AS csat_filled,
        UPPER(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE (closed_at >= TIMESTAMP '2025-01-24 01:00:00' OR requester_id <= 23)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(closed_at) OVER (PARTITION BY priority ORDER BY closed_at DESC, id) AS next_closed_at,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_id, 2) AS requester_id_vs_agent_id_avg,
        ROUND(CUME_DIST() OVER w_all_by_csat_filled, 3) AS csat_filled_cume_dist,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority)) AS distinct_agent_id_in_priority,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        LAG(csat, 2) OVER w_agent_id_csat_by_csat_filled AS prev2_csat,
        csat_filled - LAG(csat_filled) OVER (ORDER BY csat_filled ASC, id) AS csat_filled_change,
        LAG(closed_at, 3) OVER (ORDER BY csat_filled, id) AS prev3_closed_at,
        LAST_VALUE(csat_filled) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY csat_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat_filled,
        DENSE_RANK() OVER w_all_by_requester_id_ties AS requester_id_dense_rank,
        DENSE_RANK() OVER (ORDER BY csat_filled ASC) AS csat_filled_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at DESC), 3) AS closed_at_percent_rank
    FROM cleaned_tickets
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_all_by_csat_filled AS (ORDER BY csat_filled, id), w_agent_id_csat_by_csat_filled AS (PARTITION BY agent_id, csat ORDER BY csat_filled DESC, id), w_all_by_requester_id_ties AS (ORDER BY requester_id)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        closed_at,
        csat,
        csat_filled,
        channel_clean,
        next_closed_at,
        requester_id_vs_agent_id_avg,
        csat_filled_cume_dist,
        rows_in_agent_id,
        prev2_csat,
        requester_id_dense_rank,
        closed_at_percent_rank,
        SUM(csat_filled_cume_dist) OVER (PARTITION BY csat_filled_change) AS csat_filled_change_sum_csat_filled_cume_dist,
        MIN(requester_id_vs_agent_id_avg) OVER (PARTITION BY csat_filled_change) AS csat_filled_change_min_requester_id_vs_agent_id_,
        SUM(requester_id) OVER (PARTITION BY csat_filled_change) AS csat_filled_change_sum_requester_id,
        NTH_VALUE(csat_filled, 2) OVER (PARTITION BY csat_filled ORDER BY next_closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat_filled,
        COUNT(*) OVER w_csat_filled_change AS rows_in_csat_filled_change
    FROM sequenced
    WINDOW w_csat_filled_change AS (PARTITION BY csat_filled_change)
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    csat,
    csat_filled,
    requester_id_vs_agent_id_avg,
    csat_filled_cume_dist,
    rows_in_agent_id,
    prev2_csat,
    requester_id_dense_rank,
    closed_at_percent_rank,
    csat_filled_change_sum_csat_filled_cume_dist,
    csat_filled_change_min_requester_id_vs_agent_id_,
    csat_filled_change_sum_requester_id,
    rows_in_csat_filled_change,
    SUBSTRING(TRIM(channel_clean), 1, 3) AS channel_clean_prefix,
    CASE WHEN csat >= 4 THEN 'A' WHEN csat >= 2 THEN 'B' ELSE 'C' END AS csat_tier
FROM ranked
WHERE requester_id <= 18
ORDER BY csat_filled_cume_dist DESC NULLS FIRST
