WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM valid_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at >= TIMESTAMP '2025-01-19 11:00:00'
        )
      AND s.ticket_id IS NOT NULL
),
combined_details AS (
    -- Join tickets
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.ticket_agent_id
        )
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    ticket_agent_id,
    agent_hired_on,
    agent_team
FROM screened
ORDER BY actor DESC, agent_hired_on
