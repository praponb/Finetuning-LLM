WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE requester_id <= 23
      AND channel <> 'web'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(csat) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_max_3,
        RANK() OVER (PARTITION BY priority ORDER BY requester_id, id) AS requester_id_rank,
        LEAD(closed_at, 3) OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY csat, id) AS next3_closed_at,
        LAG(csat) OVER (PARTITION BY csat, TO_DATE(closed_at) ORDER BY csat, id) AS prev_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER w_priority, 0), 4) AS csat_share_of_priority,
        SUM(csat) OVER (PARTITION BY csat) AS csat_sum_csat,
        MIN(csat) OVER (PARTITION BY priority ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        DENSE_RANK() OVER (PARTITION BY priority, MONTH(closed_at) ORDER BY requester_id) AS requester_id_dense_rank,
        LAG(opened_at, 3) OVER w_priority_closed_at_month_by_2 AS prev3_opened_at,
        DENSE_RANK() OVER (ORDER BY opened_at DESC) AS opened_at_dense_rank
    FROM cleaned_tickets
    WINDOW w_priority AS (PARTITION BY priority), w_priority_closed_at_month_by_2 AS (PARTITION BY priority, MONTH(closed_at) ORDER BY requester_id, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    requester_id_rank,
    next3_closed_at,
    csat_share_of_priority,
    csat_sum_csat,
    min_csat_so_far,
    requester_id_dense_rank,
    opened_at_dense_rank
FROM with_windows
ORDER BY requester_id_dense_rank DESC NULLS FIRST, id
LIMIT 10
