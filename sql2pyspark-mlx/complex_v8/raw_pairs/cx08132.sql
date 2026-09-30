WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        UPPER(TRIM(team)) AS team_clean
    FROM support_agents
    WHERE team IS NOT NULL
      AND tier <> 'T3'
)
SELECT id AS ref_id
FROM recent
EXCEPT
SELECT ticket_id AS ref_id
FROM ticket_events
ORDER BY ref_id
