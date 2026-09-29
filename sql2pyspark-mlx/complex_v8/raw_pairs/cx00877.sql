WITH combined AS (
    -- Stack the branches
    SELECT id, hired_on
    FROM support_agents
    WHERE team IN ('Tech', 'Billing')
    UNION ALL
    SELECT id, hired_on
    FROM support_agents
    WHERE tier = 'T1'
      AND YEAR(hired_on) = 2024
)
SELECT id, COUNT(*) AS n_rows, MAX(hired_on) AS latest_hired_on
FROM combined
GROUP BY id
ORDER BY id
