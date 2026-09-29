WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        ROUND(requester_id / NULLIF(csat, 0), 3) AS requester_id_per_csat
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-02-26'
      AND opened_at < TIMESTAMP '2025-02-18 11:00:00'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        requester_id_per_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_id, csat), 0), 4) AS csat_share_of_agent_id_csat,
        DENSE_RANK() OVER (PARTITION BY agent_id ORDER BY opened_at DESC NULLS FIRST) AS opened_at_dense_rank,
        COUNT(*) OVER (PARTITION BY agent_id, csat) AS rows_in_agent_id_csat,
        ROUND(requester_id_per_csat / NULLIF(SUM(requester_id_per_csat) OVER (PARTITION BY priority, csat), 0), 4) AS requester_id_per_csat_share_of_priority_csat,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_id, csat)) AS distinct_priority_in_agent_id_csat,
        ROUND(AVG(requester_id_per_csat) OVER (ORDER BY requester_id_per_csat, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS requester_id_per_csat_avg_2,
        SIZE(COLLECT_SET(agent_id) OVER w_priority_csat) AS distinct_agent_id_in_priority_csat,
        ROUND(AVG(requester_id_per_csat) OVER (PARTITION BY csat, MONTH(opened_at) ORDER BY closed_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_per_csat_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY priority ORDER BY csat, id) AS csat_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, MONTH(opened_at) ORDER BY closed_at), 3) AS closed_at_cume_dist,
        ROUND(requester_id_per_csat - AVG(requester_id_per_csat) OVER w_agent_id_csat, 2) AS requester_id_per_csat_vs_agent_id_csat_avg,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY requester_id_per_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        ROW_NUMBER() OVER w_all_by_requester_id_per_csat_3 AS requester_id_per_csat_seq,
        MAX(requester_id_per_csat) OVER (PARTITION BY csat, MONTH(opened_at) ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS requester_id_per_csat_max_2_prior
    FROM prepared
    WINDOW w_priority_csat AS (PARTITION BY priority, csat), w_agent_id_csat AS (PARTITION BY agent_id, csat), w_all_by_requester_id_per_csat_3 AS (ORDER BY requester_id_per_csat, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        requester_id_per_csat,
        csat_share_of_agent_id_csat,
        opened_at_dense_rank,
        rows_in_agent_id_csat,
        requester_id_per_csat_share_of_priority_csat,
        distinct_priority_in_agent_id_csat,
        requester_id_per_csat_avg_2,
        distinct_agent_id_in_priority_csat,
        requester_id_per_csat_avg_6_prior,
        csat_dense_rank,
        closed_at_cume_dist,
        requester_id_per_csat_vs_agent_id_csat_avg,
        first_known_opened_at,
        requester_id_per_csat_max_2_prior
    FROM sequenced
    WHERE requester_id_per_csat_seq = 1
      AND csat < 4
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER w_csat, 2) AS csat_vs_csat_avg,
        LEAD(csat_share_of_agent_id_csat) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id) AS next_csat_share_of_agent_id_csat,
        ROW_NUMBER() OVER w_distinct_priority_in_agent_i AS opened_at_dense_rank_seq,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat
    FROM leaders
    WINDOW w_csat AS (PARTITION BY csat), w_distinct_priority_in_agent_i AS (PARTITION BY distinct_priority_in_agent_id_csat ORDER BY opened_at_dense_rank DESC, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    csat,
    requester_id_per_csat,
    csat_share_of_agent_id_csat,
    opened_at_dense_rank,
    rows_in_agent_id_csat,
    requester_id_per_csat_avg_2,
    requester_id_per_csat_avg_6_prior,
    csat_dense_rank,
    closed_at_cume_dist,
    requester_id_per_csat_vs_agent_id_csat_avg,
    first_known_opened_at,
    requester_id_per_csat_max_2_prior,
    next_csat_share_of_agent_id_csat,
    opened_at_dense_rank_seq,
    rows_in_csat,
    distinct_agent_id_in_csat,
    INITCAP(TRIM(priority)) AS priority_clean,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM windowed
ORDER BY rows_in_agent_id_csat, priority_or_unknown DESC
