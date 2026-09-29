WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND MONTH(closed_at) IN (1, 2)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        MIN(csat) OVER (PARTITION BY csat ORDER BY closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        SUM(requester_id) OVER (PARTITION BY csat) AS csat_sum_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY MONTH(closed_at))) AS distinct_agent_id_in_closed_at_month,
        ROUND(requester_id - AVG(requester_id) OVER w_csat, 2) AS requester_id_vs_csat_avg,
        MIN(requester_id) OVER (PARTITION BY csat) AS csat_min_requester_id,
        LAST_VALUE(requester_id) OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        requester_id - LAG(requester_id) OVER (ORDER BY csat DESC, id) AS requester_id_change,
        SUM(requester_id) OVER w_closed_at_month AS closed_at_month_sum_requester_id,
        LAG(csat) OVER w_agent_id_csat_by_csat AS prev_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY MONTH(closed_at)), 0), 4) AS csat_share_of_closed_at_month,
        ROUND(CUME_DIST() OVER w_csat_by_closed_at, 3) AS closed_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, csat ORDER BY csat), 3) AS csat_percent_rank,
        NTILE(5) OVER (PARTITION BY agent_id, csat ORDER BY csat, id) AS csat_quintile,
        RANK() OVER (ORDER BY csat DESC) AS csat_rank,
        ROUND(requester_id - AVG(requester_id) OVER w_closed_at_month, 2) AS requester_id_vs_closed_at_month_avg,
        COUNT(*) OVER (PARTITION BY MONTH(closed_at)) AS rows_in_closed_at_month
    FROM scoped_tickets
    WINDOW w_csat AS (PARTITION BY csat), w_closed_at_month AS (PARTITION BY MONTH(closed_at)), w_agent_id_csat_by_csat AS (PARTITION BY agent_id, csat ORDER BY csat, id), w_csat_by_closed_at AS (PARTITION BY csat ORDER BY closed_at NULLS LAST, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_agent_id_in_closed_at_month,
        prev_csat,
        MAX(prev_csat) AS max_prev_csat,
        MIN(closed_at) AS first_closed_at,
        MAX(closed_at) AS last_closed_at,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(csat_share_of_closed_at_month), 2) AS stddev_csat_share_of_closed_at_month
    FROM with_trends
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
)
SELECT
    distinct_agent_id_in_closed_at_month,
    prev_csat,
    max_prev_csat,
    first_closed_at,
    last_closed_at,
    stddev_csat_share_of_closed_at_month
FROM summary
WHERE first_closed_at < TIMESTAMP '2025-02-21 07:00:00'
ORDER BY distinct_agent_id_in_closed_at_month NULLS LAST, prev_csat
LIMIT 10
