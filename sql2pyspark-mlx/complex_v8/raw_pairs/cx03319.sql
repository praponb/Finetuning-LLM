WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week
    FROM tickets
    WHERE csat < 5
      AND agent_id <= 4
),
filtered AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.team IN ('Onboarding', 'VIP')
              AND su.hired_on < DATE '2025-02-27'
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_id, priority), 2) AS agent_id_priority_avg_requester_id,
        SUM(requester_id) OVER (PARTITION BY agent_id, priority) AS agent_id_priority_sum_requester_id,
        ROUND((UNIX_TIMESTAMP(closed_at_week) - UNIX_TIMESTAMP(LAG(closed_at_week) OVER (ORDER BY closed_at_week DESC, id))) / 60, 1) AS minutes_since_prev_closed_at_week
    FROM filtered
    WINDOW w_agent_id_priority AS (PARTITION BY agent_id, priority)
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        PERCENTILE_APPROX(agent_id_priority_avg_requester_id, 0.5) AS median_agent_id_priority_avg_requester_id,
        MIN(opened_at) AS first_opened_at
    FROM with_trends
    GROUP BY 1
)
-- Final projection
SELECT
    closed_at_month,
    first_opened_at,
    DATE_TRUNC('DAY', closed_at_month) AS closed_at_month_day
FROM summary
ORDER BY first_opened_at DESC NULLS FIRST, closed_at_month
LIMIT 5
