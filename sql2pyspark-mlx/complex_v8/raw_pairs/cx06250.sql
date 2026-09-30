WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        csat,
        closed_at + INTERVAL 7 DAYS AS closed_at_plus_7d
    FROM tickets
    WHERE priority <> 'P2'
      AND csat <= 4
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY TO_DATE(opened_at), TO_DATE(closed_at_plus_7d) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        SUM(csat) OVER (PARTITION BY TO_DATE(opened_at), TO_DATE(closed_at_plus_7d) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY TO_DATE(opened_at), TO_DATE(closed_at_plus_7d) ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY csat), 0), 4) AS requester_id_share_of_csat,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat), 2) AS csat_avg_requester_id
    FROM scoped_tickets
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at_plus_7d) ORDER BY opened_at, id) AS next_requester_id,
        ROUND(AVG(csat_avg_requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at_plus_7d) ORDER BY opened_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_requester_id_avg_3,
        SUM(csat) OVER (PARTITION BY MONTH(opened_at), csat) AS opened_at_month_csat_sum_csat,
        LEAD(second_requester_id, 3, 0) OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at_plus_7d) ORDER BY opened_at, id) AS next3_second_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', closed_at_plus_7d) ORDER BY opened_at), 3) AS opened_at_cume_dist
    FROM with_windows
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, csat_avg_requester_id, csat_avg_requester_id_avg_3, opened_at_cume_dist
    FROM with_trends
) src
UNPIVOT (
    value FOR measure IN (csat_avg_requester_id AS m_csat_avg_requester_id, csat_avg_requester_id_avg_3 AS csat_avg_requester_id_avg_3_v, opened_at_cume_dist AS opened_at_cume_dist_v)
) unp
