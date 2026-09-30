WITH combined AS (
    -- Stack the branches
    SELECT
        'claim' AS origin,
        id AS ref_id,
        reported_at AS happened_at,
        claimed_amount AS metric
    FROM claims
    WHERE status = 'REJECTED'
      AND claimed_amount < 30001.6
    UNION ALL
    SELECT
        'claim_2' AS origin,
        id,
        reported_at,
        approved_amount
    FROM claims
    WHERE claim_type <> 'medical'
      AND policy_id <= 29
)
SELECT origin, COUNT(*) AS n_rows, SUM(metric) AS total_metric, MAX(happened_at) AS latest_happened_at
FROM combined
GROUP BY origin
ORDER BY origin
