WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter
    FROM tickets
    WHERE (priority = 'P1' OR closed_at >= TIMESTAMP '2025-01-17 21:00:00')
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        closed_at_year,
        LEAD(requester_id, 3, 0) OVER w_priority_agent_id_by_opened AS next3_requester_id,
        MIN(requester_id) OVER (PARTITION BY priority, agent_id ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_requester_id_so_far
    FROM prepared_tickets
    WINDOW w_priority_agent_id_by_opened AS (PARTITION BY priority, agent_id ORDER BY opened_at DESC, id)
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY agent_id, priority ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(next3_requester_id) OVER (ORDER BY opened_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next3_requester_id_max_3,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority), 2) AS requester_id_vs_priority_avg
    FROM with_trends
)
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    closed_at_year,
    min_requester_id_so_far,
    next3_requester_id_max_3,
    requester_id_vs_priority_avg,
    CONCAT_WS('|', priority, channel) AS priority_channel_label,
    UPPER(TRIM(priority)) AS priority_clean
FROM with_trends_2
ORDER BY priority_clean, id
LIMIT 20
