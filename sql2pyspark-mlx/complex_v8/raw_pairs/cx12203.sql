WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM tickets
    WHERE requester_id > 9
),
with_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
stats AS (
    -- Group-level totals
    SELECT
        agent_id,
        agent_team,
        DATE_TRUNC('MONTH', agent_hired_on) AS agent_hired_on_month,
        SUM(requester_id) AS total_requester_id,
        ROUND(100 * COUNT(CASE WHEN agent_team IN ('Billing', 'Tech', 'VIP') THEN 1 END) / COUNT(*), 1) AS pct_agent_team_in_billing_tech_v,
        ROUND(100 * COUNT(CASE WHEN priority IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_priority_is_not_null,
        COUNT(*) FILTER (WHERE agent_team = 'Onboarding') AS n_agent_team_onboarding,
        MAX(requester_id) AS max_requester_id,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM with_details_agent
    WHERE agent_hired_on IS NOT NULL
    GROUP BY agent_id, agent_team, DATE_TRUNC('MONTH', agent_hired_on)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(CASE WHEN channel IS NOT NULL THEN 1 END) AS ticket_channel_count,
        COUNT(*) AS ticket_count,
        SUM(csat) AS total_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.total_ticket_csat
    FROM stats s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_id, agent_team, metric_name, value
FROM (
    SELECT agent_id, agent_team, pct_agent_team_in_billing_tech_v, pct_priority_is_not_null
    FROM summarized_rows
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric_name IN (pct_agent_team_in_billing_tech_v, pct_priority_is_not_null)
) unp
