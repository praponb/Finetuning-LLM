WITH combined AS (
    -- Stack the branches
    SELECT status, reported_at AS happened_at, claim_type AS label
    FROM claims
    UNION
    SELECT status, reported_at, claim_type
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_date BETWEEN DATE '2024-03-29' AND DATE '2025-11-08'
)
SELECT status, COUNT(*) AS n_rows, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY status
ORDER BY status
