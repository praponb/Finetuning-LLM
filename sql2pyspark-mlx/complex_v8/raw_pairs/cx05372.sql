WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE claim_type = 'flood'
),
unioned AS (
    -- Stack the branches
    SELECT policy_id, reported_at AS happened_at, status
    FROM filtered
    WHERE status = 'PAID'
    UNION ALL
    SELECT adjuster_id, reported_at, claim_type
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-04-14' AND DATE '2025-04-17'
      AND policy_id <= 17
    UNION ALL
    SELECT adjuster_id, reported_at, status
    FROM claims
    WHERE claimed_amount BETWEEN 11235.6 AND 32441.2
)
SELECT status, COUNT(*) AS n_rows, SUM(policy_id) AS total_policy_id, MAX(happened_at) AS latest_happened_at
FROM unioned
GROUP BY status
ORDER BY status
