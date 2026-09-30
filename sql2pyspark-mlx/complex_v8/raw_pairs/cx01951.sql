WITH filtered_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        COALESCE(csat, 0) AS csat_filled,
        TO_DATE(opened_at) AS opened_at_date,
        csat - requester_id AS csat_minus_requester_id
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND TO_DATE(opened_at) < DATE '2025-02-24'
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.opened_at_date,
        s.csat_minus_requester_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', agent_hired_on) AS agent_hired_on_month,
        agent_team,
        MIN(opened_at_date) AS first_opened_at_date,
        SUM(csat) AS total_csat,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        SUM(csat_minus_requester_id) AS total_csat_minus_requester_id,
        MAX(csat) AS max_csat
    FROM combined_details_agent
    GROUP BY DATE_TRUNC('MONTH', agent_hired_on), agent_team
)
SELECT *
FROM (
    SELECT first_opened_at_date, agent_team, max_csat
    FROM stats
) src
PIVOT (
    COUNT(max_csat)
    FOR agent_team IN ('Billing', 'VIP', 'Tech', 'Onboarding')
)
ORDER BY first_opened_at_date
