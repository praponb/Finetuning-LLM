WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at
    FROM claims
    WHERE claimed_amount BETWEEN 3192.1 AND 42838.9
      AND claim_date < DATE '2025-01-24'
)
SELECT id, reported_at
FROM base
WHERE MONTH(reported_at) IN (1, 3)
  AND policy_id >= 21
UNION ALL
SELECT adjuster_id AS id, reported_at
FROM claims
ORDER BY reported_at
