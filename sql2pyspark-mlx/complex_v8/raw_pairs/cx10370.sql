WITH cleaned_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        SUBSTRING(TRIM(tier), 1, 2) AS tier_prefix
    FROM support_agents
    WHERE agent_name IS NOT NULL
)
SELECT team
FROM cleaned_agents
WHERE hired_on < DATE '2025-02-26'
  AND team = 'VIP'
INTERSECT
SELECT actor
FROM ticket_events
ORDER BY team
