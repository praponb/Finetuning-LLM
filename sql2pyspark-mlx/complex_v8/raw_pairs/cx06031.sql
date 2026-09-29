WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        ABS(requester_id - 18) AS requester_id_distance
    FROM tickets
    WHERE (priority = 'P4' OR MONTH(opened_at) IN (1, 3))
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(opened_at, 2) OVER (PARTITION BY agent_id, csat ORDER BY csat, id) AS next2_opened_at,
        MIN(requester_id_distance) OVER (PARTITION BY MONTH(closed_at), DATE_TRUNC('MONTH', opened_at)) AS closed_at_month_opened_at_month_min_requester_id,
        LEAD(closed_at, 3) OVER (ORDER BY requester_id_distance, id) AS next3_closed_at,
        SUM(requester_id_distance) OVER (PARTITION BY agent_id, csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_distance,
        DENSE_RANK() OVER (ORDER BY requester_id_distance) AS requester_id_distance_dense_rank,
        ROUND(AVG(csat) OVER (ORDER BY requester_id_distance, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        LAST_VALUE(closed_at) OVER (ORDER BY requester_id_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        COUNT(*) OVER w_closed_at_month_opened_at_mo AS rows_in_closed_at_month_opened_at_month
    FROM valid_tickets
    WINDOW w_closed_at_month_opened_at_mo AS (PARTITION BY MONTH(closed_at), DATE_TRUNC('MONTH', opened_at))
)
SELECT
    id,
    agent_id,
    channel,
    csat,
    requester_id_distance,
    next2_opened_at,
    running_requester_id_distance,
    requester_id_distance_dense_rank,
    csat_avg_2,
    last_closed_at,
    rows_in_closed_at_month_opened_at_month,
    GREATEST(csat, requester_id_distance) AS greatest_csat_requester_id_distance
FROM ranked
WHERE agent_id IS NOT NULL
ORDER BY csat_avg_2 DESC
