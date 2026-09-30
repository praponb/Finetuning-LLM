WITH base AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CASE WHEN tier IN ('T2', 'T1') THEN 'group_a' WHEN tier = 'T3' THEN 'group_b' ELSE 'other' END AS tier_group
    FROM support_agents
    WHERE hired_on < DATE '2024-12-31'
)
SELECT id, team
FROM base
INTERSECT ALL
SELECT csat, channel
FROM tickets
WHERE TO_DATE(opened_at) < DATE '2025-02-06'
  AND closed_at < TIMESTAMP '2025-02-26 16:00:00'
ORDER BY team, id
