WITH combined AS (
    -- Stack the branches
    SELECT id, reported_at AS happened_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 8 AND 14
      AND claim_type <> 'theft'
    UNION ALL
    SELECT adjuster_id, reported_at
    FROM claims
)
SELECT id, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY id
ORDER BY id
