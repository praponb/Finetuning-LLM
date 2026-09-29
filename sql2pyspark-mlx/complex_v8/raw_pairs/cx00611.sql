WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE (ARRAY_CONTAINS(tags, 'login') OR agent_id <= 6)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_at,
        channel,
        csat,
        ROUND(csat / NULLIF(SUM(csat) OVER w_csat, 0), 4) AS csat_share_of_csat,
        ROW_NUMBER() OVER (ORDER BY opened_at DESC, id) AS rn,
        FIRST_VALUE(requester_id) OVER (PARTITION BY channel, priority ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        csat - LAG(csat) OVER (PARTITION BY priority ORDER BY csat DESC, id) AS csat_change,
        RANK() OVER (ORDER BY opened_at DESC, id) AS opened_at_rank,
        MAX(requester_id) OVER w_csat AS csat_max_requester_id,
        ROUND(AVG(csat) OVER (PARTITION BY channel, priority ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_2_prior,
        requester_id - LAG(requester_id) OVER w_all_by_opened_at AS requester_id_change,
        MAX(csat) OVER (PARTITION BY priority ORDER BY csat DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_max_3,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY csat), 2) AS requester_id_vs_csat_avg,
        DENSE_RANK() OVER (PARTITION BY agent_id, channel ORDER BY csat NULLS LAST) AS csat_dense_rank,
        LEAD(requester_id, 2) OVER (ORDER BY opened_at DESC, id) AS next2_requester_id,
        MIN(requester_id) OVER (PARTITION BY channel) AS channel_min_requester_id,
        ROUND(AVG(csat) OVER w_csat, 2) AS csat_avg_csat,
        SUM(csat) OVER (PARTITION BY agent_id, channel ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM cleaned_tickets
    WINDOW w_csat AS (PARTITION BY csat), w_all_by_opened_at AS (ORDER BY opened_at DESC, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(running_csat) OVER (ORDER BY opened_at_rank, id) AS next_running_csat,
        ROUND(csat_share_of_csat - AVG(csat_share_of_csat) OVER (PARTITION BY csat_change, csat_dense_rank), 2) AS csat_share_of_csat_vs_csat_change_csat_dense_ran,
        SUM(opened_at_rank) OVER (ORDER BY opened_at_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_opened_at_rank,
        ROUND(AVG(requester_id_vs_csat_avg) OVER (ORDER BY csat_change DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_vs_csat_avg_avg_6_prior,
        LEAD(first_requester_id) OVER w_all_by_csat_change AS next_first_requester_id
    FROM with_trends
    WINDOW w_all_by_csat_change AS (ORDER BY csat_change DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    channel,
    first_requester_id,
    csat_change,
    opened_at_rank,
    csat_max_requester_id,
    csat_avg_2_prior,
    requester_id_change,
    csat_max_3,
    requester_id_vs_csat_avg,
    next2_requester_id,
    channel_min_requester_id,
    running_csat,
    csat_share_of_csat_vs_csat_change_csat_dense_ran,
    requester_id_vs_csat_avg_avg_6_prior,
    next_first_requester_id,
    QUARTER(opened_at) AS opened_at_quarter,
    DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
FROM ranked
ORDER BY first_requester_id DESC
