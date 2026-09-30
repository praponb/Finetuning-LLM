WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE agent_id >= 7
      AND priority IN ('P1', 'P3', 'P4')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority, csat), 2) AS csat_vs_priority_csat_avg,
        SUM(requester_id) OVER (PARTITION BY channel, csat ORDER BY csat, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS requester_id_sum_6_prior,
        MAX(csat) OVER (PARTITION BY priority, csat) AS priority_csat_max_csat,
        ROUND(CUME_DIST() OVER w_all_by_requester_id, 3) AS requester_id_cume_dist,
        COUNT(*) OVER (PARTITION BY priority ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER w_priority_csat AS rows_in_priority_csat,
        SUM(csat) OVER (PARTITION BY priority, csat) AS priority_csat_sum_csat,
        ROUND(requester_id - AVG(requester_id) OVER w_priority_csat, 2) AS requester_id_vs_priority_csat_avg,
        ROUND(AVG(csat) OVER (ORDER BY requester_id, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        RANK() OVER w_priority_by_csat_ties AS csat_rank,
        LAST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY priority ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_csat,
        NTILE(10) OVER (PARTITION BY priority ORDER BY csat DESC, id) AS csat_decile,
        requester_id - LAG(requester_id) OVER (ORDER BY requester_id, id) AS requester_id_change
    FROM recent
    WINDOW w_all_by_requester_id AS (ORDER BY requester_id, id), w_priority_csat AS (PARTITION BY priority, csat), w_priority_by_csat_ties AS (PARTITION BY priority ORDER BY csat DESC)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    channel,
    tags,
    csat_vs_priority_csat_avg,
    requester_id_sum_6_prior,
    priority_csat_max_csat,
    rows_in_priority_csat,
    priority_csat_sum_csat,
    requester_id_vs_priority_csat_avg,
    csat_avg_2,
    csat_rank,
    last_known_csat,
    requester_id_change
FROM sequenced
ORDER BY requester_id_change
