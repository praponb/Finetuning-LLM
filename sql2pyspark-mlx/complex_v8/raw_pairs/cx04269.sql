WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        closed_at + INTERVAL 30 MINUTES AS closed_at_plus_30m
    FROM tickets
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_closed_at_month_closed_at_pl, 3) AS closed_at_plus_30m_percent_rank,
        DENSE_RANK() OVER w_all_by_closed_at_ties AS closed_at_dense_rank,
        ROW_NUMBER() OVER (ORDER BY requester_id NULLS LAST, id) AS requester_id_seq,
        COUNT(*) OVER w_opened_at_month AS rows_in_opened_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY closed_at_plus_30m), 3) AS closed_at_plus_30m_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_at) ORDER BY closed_at_plus_30m, id), 3) AS closed_at_plus_30m_cume_dist_2,
        NTILE(5) OVER w_all_by_closed_at AS closed_at_quintile,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id), 0), 4) AS requester_id_share_of_closed_at_year_agent_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id), 2) AS closed_at_year_agent_id_avg_requester_id,
        SUM(requester_id) OVER (PARTITION BY YEAR(closed_at), agent_id) AS closed_at_year_agent_id_sum_requester_id,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        ROUND((UNIX_TIMESTAMP(closed_at_plus_30m) - UNIX_TIMESTAMP(LAG(closed_at_plus_30m) OVER (ORDER BY closed_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_closed_at_plus_30m,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (ORDER BY requester_id NULLS LAST, id))) / 60, 1) AS minutes_since_prev_closed_at,
        requester_id - LAG(requester_id) OVER (ORDER BY closed_at DESC NULLS FIRST, id) AS requester_id_change,
        ROUND(CUME_DIST() OVER (ORDER BY requester_id ASC), 3) AS requester_id_cume_dist
    FROM filtered
    WINDOW w_closed_at_month_closed_at_pl AS (PARTITION BY DATE_TRUNC('MONTH', closed_at), DATE_TRUNC('MONTH', closed_at_plus_30m) ORDER BY closed_at_plus_30m ASC, id), w_all_by_closed_at_ties AS (ORDER BY closed_at DESC NULLS FIRST), w_opened_at_month AS (PARTITION BY DATE_TRUNC('MONTH', opened_at)), w_all_by_closed_at AS (ORDER BY closed_at DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    closed_at,
    closed_at_plus_30m,
    closed_at_plus_30m_percent_rank,
    closed_at_dense_rank,
    requester_id_seq,
    closed_at_plus_30m_cume_dist,
    closed_at_plus_30m_cume_dist_2,
    closed_at_quintile,
    closed_at_year_agent_id_avg_requester_id,
    first_known_closed_at,
    minutes_since_prev_closed_at,
    requester_id_cume_dist,
    DATE_TRUNC('MONTH', opened_at) AS opened_at_month
FROM with_windows
ORDER BY closed_at_quintile DESC NULLS FIRST, closed_at_plus_30m_percent_rank, id
LIMIT 10
