WITH scoped AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE channel = 'phone'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        MAX(requester_id) OVER (PARTITION BY TO_DATE(closed_at), priority) AS closed_at_to_date_priority_max_requester_id,
        DENSE_RANK() OVER w_agent_id_priority_by_csat AS csat_dense_rank,
        MAX(requester_id) OVER (PARTITION BY csat) AS csat_max_requester_id,
        DENSE_RANK() OVER w_all_by_closed_at_ties AS closed_at_dense_rank,
        RANK() OVER (PARTITION BY agent_id, priority ORDER BY csat DESC) AS csat_rank,
        ROUND(requester_id - AVG(requester_id) OVER w_csat, 2) AS requester_id_vs_csat_avg,
        ROW_NUMBER() OVER (PARTITION BY csat ORDER BY closed_at, id) AS closed_at_seq,
        RANK() OVER (PARTITION BY csat, YEAR(opened_at) ORDER BY csat) AS csat_rank_2,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, priority ORDER BY csat DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_2_prior
    FROM scoped
    WINDOW w_agent_id_priority_by_csat AS (PARTITION BY agent_id, priority ORDER BY csat DESC, id), w_all_by_closed_at_ties AS (ORDER BY closed_at), w_csat AS (PARTITION BY csat)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        priority,
        closed_at_to_date_priority_max_requester_id,
        csat_max_requester_id,
        closed_at_dense_rank,
        csat_rank,
        requester_id_vs_csat_avg,
        csat_rank_2,
        requester_id_avg_2_prior,
        NTILE(3) OVER w_all_by_csat_rank AS csat_rank_tercile,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY csat_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY csat_rank_2)) AS distinct_priority_in_csat_rank_2,
        NTILE(3) OVER (PARTITION BY priority, csat_rank_2 ORDER BY requester_id, id) AS requester_id_tercile,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat_rank, id), 3) AS csat_rank_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY csat_rank, id), 3) AS csat_rank_cume_dist,
        SUM(requester_id_avg_2_prior) OVER (PARTITION BY closed_at_dense_rank ORDER BY requester_id DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS requester_id_avg_2_prior_sum_3_prior
    FROM metrics
    WINDOW w_all_by_csat_rank AS (ORDER BY csat_rank, id)
)
SELECT
    id,
    priority,
    closed_at_to_date_priority_max_requester_id,
    csat_max_requester_id,
    csat_rank,
    requester_id_vs_csat_avg,
    csat_rank_2,
    requester_id_avg_2_prior,
    csat_rank_tercile,
    second_requester_id,
    distinct_priority_in_csat_rank_2,
    requester_id_tercile,
    csat_rank_percent_rank,
    requester_id_avg_2_prior_sum_3_prior,
    CASE WHEN csat_max_requester_id >= 188 THEN 'HIGH' WHEN csat_max_requester_id >= 63 THEN 'MEDIUM' ELSE 'LOW' END AS csat_max_requester_id_tier,
    FLOOR(requester_id_vs_csat_avg / 10) * 10 AS requester_id_vs_csat_avg_bucket
FROM with_trends
ORDER BY requester_id_avg_2_prior NULLS LAST, id
LIMIT 10
