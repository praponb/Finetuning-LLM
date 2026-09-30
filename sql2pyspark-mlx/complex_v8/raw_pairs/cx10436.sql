WITH unioned AS (
    -- Stack the branches
    SELECT policy_id AS key_id, reported_at
    FROM claims
    UNION
    SELECT adjuster_id, reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
      AND adjuster_id >= 2
)
SELECT key_id, COUNT(*) AS n_rows, MAX(reported_at) AS latest_reported_at
FROM unioned
GROUP BY key_id
ORDER BY key_id
