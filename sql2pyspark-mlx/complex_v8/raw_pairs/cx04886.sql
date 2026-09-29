WITH valid_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        QUARTER(closed_at) AS closed_at_quarter
    FROM tickets
    WHERE (csat <= 4 OR requester_id <= 21)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        closed_at_quarter,
        ROUND(CUME_DIST() OVER (PARTITION BY closed_at_quarter, csat ORDER BY closed_at, id), 3) AS closed_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY closed_at_quarter, csat ORDER BY closed_at) AS closed_at_dense_rank,
        LAST_VALUE(closed_at_quarter) OVER (ORDER BY closed_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_quarter,
        ROUND(CUME_DIST() OVER (PARTITION BY csat ORDER BY closed_at DESC), 3) AS closed_at_cume_dist_2,
        LEAD(requester_id, 3) OVER (PARTITION BY closed_at_quarter, csat ORDER BY closed_at, id) AS next3_requester_id,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        FIRST_VALUE(closed_at) OVER (PARTITION BY closed_at_quarter, priority ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY closed_at_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        COUNT(*) OVER (PARTITION BY priority, closed_at_quarter) AS rows_in_priority_closed_at_quarter
    FROM valid_tickets
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags,
        closed_at_dense_rank,
        last_closed_at_quarter,
        next3_requester_id,
        first_closed_at,
        first_known_requester_id,
        last_known_closed_at,
        rows_in_priority_closed_at_quarter,
        CONCAT_WS(': ', channel, priority) AS channel_priority_label
    FROM windowed
)
SELECT DISTINCT id, rows_in_priority_closed_at_quarter
FROM tagged
WHERE first_closed_at >= TIMESTAMP '2025-01-14 03:00:00'
ORDER BY rows_in_priority_closed_at_quarter, id NULLS LAST
