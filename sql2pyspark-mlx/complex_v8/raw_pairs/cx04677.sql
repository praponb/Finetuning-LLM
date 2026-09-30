WITH unioned AS (
    -- Stack the branches
    SELECT team, id AS ref_id, 'agent' AS src
    FROM support_agents
    WHERE tier IN ('T3', 'T1')
    UNION
    SELECT tier AS team, id AS ref_id, 'agent_2' AS src
    FROM support_agents
)
SELECT src, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM unioned
GROUP BY src
ORDER BY src
