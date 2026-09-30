WITH cleaned AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE requester_id < 18
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(closed_at) AS closed_at_date,
        DAYOFWEEK(opened_at) AS opened_at_dayofweek,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at
    FROM cleaned
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.csat,
        s.tags,
        s.closed_at_date,
        s.opened_at_dayofweek,
        su.team AS agent_team
    FROM tagged s
    JOIN support_agents su ON su.id = s.agent_id AND YEAR(su.hired_on) = 2024
)
SELECT *
FROM (
    SELECT opened_at, opened_at_dayofweek, agent_team, csat
    FROM combined_details_agent
) src
PIVOT (
    SUM(csat)
    FOR agent_team IN ('Billing', 'Onboarding', 'Tech')
)
