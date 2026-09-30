WITH combined AS (
    -- Stack the branches
    SELECT adjuster_id AS ref_id, status
    FROM claims
    WHERE claim_date >= DATE '2024-06-13'
      AND policy_id >= 24
    UNION ALL
    SELECT policy_id, claim_type
    FROM claims
    UNION ALL
    SELECT holder_id, status
    FROM policies
    WHERE holder_id <= 2
      AND start_date IS NOT NULL
)
SELECT status, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM combined
GROUP BY status
ORDER BY status
