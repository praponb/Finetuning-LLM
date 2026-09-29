WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
    WHERE channel NOT IN ('phone', 'chat')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY priority, agent_id), 2) AS requester_id_vs_priority_agent_id_avg,
        ROUND(AVG(csat) OVER (ORDER BY closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3,
        LAST_VALUE(closed_at) OVER (PARTITION BY channel, csat ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at
    FROM base_tickets
),
agent_id_stats AS (
    -- Avg requester_id per agent_id
    SELECT agent_id, AVG(requester_id) AS avg_requester_id
    FROM metrics
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_requester_id,
        ROUND(s.requester_id - g.avg_requester_id, 2) AS requester_id_minus_avg
    FROM metrics s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.requester_id > g.avg_requester_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.priority <> 'P4') FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count
    FROM outliers s
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.csat_avg_3,
        s.last_closed_at,
        s.avg_requester_id,
        s.requester_id_minus_avg,
        s.ticket_priority_count,
        su.agent_name
    FROM profiled s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT *
FROM (
    SELECT agent_id, channel, avg_requester_id
    FROM combined_details_agent
) src
PIVOT (
    MAX(avg_requester_id)
    FOR channel IN ('email', 'web', 'phone')
)
ORDER BY agent_id
