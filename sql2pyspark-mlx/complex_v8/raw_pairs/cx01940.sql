WITH combined AS (
    -- Stack the branches
    SELECT tier, hired_on AS day, id
    FROM support_agents
    WHERE hired_on < DATE '2025-02-26'
      AND team NOT IN ('Tech', 'Onboarding')
    UNION ALL
    SELECT team, hired_on, id
    FROM support_agents
    WHERE team IN ('VIP', 'Onboarding', 'Tech')
      AND tier = 'T1'
)
SELECT tier, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(day) AS latest_day
FROM combined
GROUP BY tier
ORDER BY tier
