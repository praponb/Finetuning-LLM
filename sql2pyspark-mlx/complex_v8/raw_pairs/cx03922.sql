WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        CONCAT_WS('|', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE TO_DATE(closed_at) < DATE '2025-03-04'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        channel_priority_label,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_share_of_agent_id,
        NTILE(5) OVER (ORDER BY opened_at DESC NULLS FIRST, id) AS opened_at_quintile,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at)), 2) AS requester_id_vs_agent_id_closed_at_month_avg,
        LAST_VALUE(closed_at) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        FIRST_VALUE(closed_at) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        LAST_VALUE(closed_at) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_3,
        NTH_VALUE(requester_id, 2) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_7,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        requester_id - LAG(requester_id) OVER (PARTITION BY agent_id ORDER BY requester_id, id) AS requester_id_change,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_id), 2) AS requester_id_vs_agent_id_avg,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_2,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_agent_id_closed_at_month, 0), 4) AS requester_id_share_of_agent_id_closed_at_month
    FROM recent
    WINDOW w_agent_id_closed_at_month AS (PARTITION BY agent_id, DATE_TRUNC('MONTH', closed_at))
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', last_known_closed_at) AS last_known_closed_at_month,
        agent_id,
        DATE_TRUNC('MONTH', first_closed_at) AS first_closed_at_month,
        MIN(last_closed_at_3) AS first_last_closed_at_3,
        MAX(second_closed_at) AS last_second_closed_at,
        ROUND(SUM(requester_id_share_of_agent_id), 2) AS total_requester_id_share_of_agent_id,
        MIN(second_closed_at) AS first_second_closed_at
    FROM windowed
    GROUP BY DATE_TRUNC('MONTH', last_known_closed_at), agent_id, DATE_TRUNC('MONTH', first_closed_at)
)
-- Final projection
SELECT
    last_known_closed_at_month,
    first_closed_at_month,
    first_last_closed_at_3,
    last_second_closed_at,
    first_second_closed_at
FROM stats
ORDER BY last_second_closed_at
