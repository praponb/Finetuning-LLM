WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        DATE_TRUNC('YEAR', opened_at) AS opened_at_year,
        DATE_TRUNC('HOUR', opened_at) AS opened_at_hour
    FROM tickets
    WHERE (priority = 'P3' OR requester_id <= 15)
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM cleaned_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.opened_at_year,
        s.opened_at_hour,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM without_matches s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    priority,
    channel,
    opened_at_year,
    opened_at_hour,
    agent_name,
    agent_team,
    agent_hired_on
FROM combined_details_agent
WHERE opened_at_year >= TIMESTAMP '2025-01-09 09:00:00'
ORDER BY opened_at_year DESC
