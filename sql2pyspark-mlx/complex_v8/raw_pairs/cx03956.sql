WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT adjuster_id, reported_at, claim_type
FROM claims
WHERE claimed_amount <= 42447.2
  AND status <> 'REJECTED'
UNION ALL
SELECT id, reported_at, status
FROM claims
WHERE claim_date >= DATE '2024-04-15'
  AND claimed_amount BETWEEN 11235.6 AND 42838.9
UNION ALL
SELECT policy_id, reported_at, status
FROM claims
WHERE claimed_amount > 10518.4
ORDER BY adjuster_id DESC, reported_at
