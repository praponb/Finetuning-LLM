WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        SUBSTRING(TRIM(team), 1, 4) AS team_prefix
    FROM support_agents
)
SELECT id AS entity_id, team_prefix AS category, 'agent' AS source
FROM cleaned
WHERE tier NOT IN ('T3', 'T1')
  AND team_prefix IS NOT NULL
UNION ALL
SELECT ticket_id, event_type, 'event' AS source
FROM ticket_events
WHERE event_at >= TIMESTAMP '2025-01-31 13:00:00'
ORDER BY source DESC
