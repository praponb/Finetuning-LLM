WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE agent_id <= 7
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY MONTH(closed_at), csat), 0), 4) AS requester_id_share_of_closed_at_month_csat,
        MAX(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_requester_id_so_far,
        MAX(requester_id) OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS requester_id_max_4,
        requester_id - LAG(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id DESC, id) AS requester_id_change,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id DESC) AS requester_id_rank,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY MONTH(closed_at), csat)) AS distinct_agent_id_in_closed_at_month_csat,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id DESC), 3) AS requester_id_cume_dist,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY MONTH(closed_at), csat), 2) AS requester_id_vs_closed_at_month_csat_avg,
        SUM(requester_id) OVER w_closed_at_month_csat AS closed_at_month_csat_sum_requester_id,
        DENSE_RANK() OVER (PARTITION BY channel, DATE_TRUNC('MONTH', closed_at) ORDER BY csat, id) AS csat_dense_rank,
        ROUND(PERCENT_RANK() OVER w_csat_agent_id_by_csat_ties, 3) AS csat_percent_rank,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY priority ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id
    FROM filtered_tickets
    WINDOW w_closed_at_month_csat AS (PARTITION BY MONTH(closed_at), csat), w_csat_agent_id_by_csat_ties AS (PARTITION BY csat, agent_id ORDER BY csat)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(first_known_closed_at), csat ORDER BY closed_at_month_csat_sum_requester_id DESC), 3) AS closed_at_month_csat_sum_requester_id_percent_ra,
        LAG(csat, 1, 0) OVER (ORDER BY csat_percent_rank DESC, id) AS prev_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat_percent_rank DESC), 3) AS csat_percent_rank_percent_rank,
        ROUND(requester_id_max_4 - AVG(requester_id_max_4) OVER (PARTITION BY agent_id, requester_id_rank), 2) AS requester_id_max_4_vs_agent_id_requester_id_rank
    FROM with_windows
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    csat,
    tags,
    max_requester_id_so_far,
    requester_id_max_4,
    requester_id_change,
    requester_id_rank,
    first_known_closed_at,
    requester_id_vs_closed_at_month_csat_avg,
    closed_at_month_csat_sum_requester_id,
    csat_dense_rank,
    csat_percent_rank,
    closed_at_month_csat_sum_requester_id_percent_ra,
    prev_csat,
    requester_id_max_4_vs_agent_id_requester_id_rank,
    ROUND(requester_id_cume_dist * 0.25, 2) AS requester_id_cume_dist_adj
FROM ranked
ORDER BY closed_at_month_csat_sum_requester_id, first_known_closed_at DESC
