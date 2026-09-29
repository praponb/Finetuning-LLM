WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at,
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 10 AND 14
      AND MONTH(closed_at) IN (2, 3)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(*) FILTER (WHERE priority IS NOT NULL) AS ticket_priority_count,
        COUNT(*) FILTER (WHERE channel IS NOT NULL) AS ticket_channel_count,
        COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM valid_tickets s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
with_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.days_since_opened_at,
        s.channel_priority_label,
        s.channel_prefix,
        s.ticket_priority_count,
        s.ticket_channel_count,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM summarized_rows s
    JOIN support_agents su ON su.id = s.agent_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_team,
        COUNT(*) FILTER (WHERE priority = 'P4') AS n_priority_p4,
        MAX(days_since_opened_at) AS max_days_since_opened_at,
        COUNT(*) AS row_count
    FROM with_details_agent
    GROUP BY 1
    HAVING COUNT(*) >= 2
)
SELECT *
FROM (
    SELECT row_count, agent_team, n_priority_p4
    FROM summary
) src
PIVOT (
    AVG(n_priority_p4)
    FOR agent_team IN ('Onboarding', 'Tech', 'VIP', 'Billing')
)
ORDER BY row_count
