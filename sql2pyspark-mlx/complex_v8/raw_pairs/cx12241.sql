WITH combined AS (
    -- Stack the branches
    SELECT adjuster_id, policy_id, status AS label
    FROM claims
    UNION ALL
    SELECT id, holder_id, status
    FROM policies
    WHERE product NOT IN ('HEALTH', 'LIFE')
      AND holder_id IS NOT NULL
)
SELECT label, COUNT(*) AS n_rows, SUM(adjuster_id) AS total_adjuster_id
FROM combined
GROUP BY label
ORDER BY label
