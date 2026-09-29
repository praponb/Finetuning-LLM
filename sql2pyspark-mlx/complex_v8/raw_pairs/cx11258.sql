WITH unioned AS (
    -- Stack the branches
    SELECT level, id, 'adjuster' AS src
    FROM adjusters
    UNION ALL
    SELECT adjuster_id, policy_id, 'claim' AS src
    FROM claims
    WHERE policy_id >= 26
      AND adjuster_id IS NOT NULL
    UNION ALL
    SELECT policy_id, adjuster_id, 'claim' AS src
    FROM claims
    WHERE adjuster_id <= 7
)
SELECT src, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM unioned
GROUP BY src
ORDER BY src
