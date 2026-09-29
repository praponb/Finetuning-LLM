WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags,
        DATEDIFF(DATE '2025-12-31', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE priority = 'P3'
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.channel,
        s.days_since_closed_at,
        su.agent_name
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
agent_id_stats AS (
    -- Avg days_since_closed_at per agent_id
    SELECT agent_id, AVG(days_since_closed_at) AS avg_days_since_closed_at
    FROM enriched_agent
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_closed_at,
        ROUND(s.days_since_closed_at - g.avg_days_since_closed_at, 2) AS days_since_closed_at_minus_avg
    FROM enriched_agent s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.days_since_closed_at > g.avg_days_since_closed_at
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT agent_id, channel, days_since_closed_at_minus_avg
    FROM above_average
) src
PIVOT (
    MIN(days_since_closed_at_minus_avg)
    FOR channel IN ('web', 'email')
)
ORDER BY agent_id
