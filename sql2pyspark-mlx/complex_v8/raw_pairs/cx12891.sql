WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE claim_type IS NOT NULL
      AND reported_at >= TIMESTAMP '2025-02-06 09:00:00'
)
SELECT adjuster_id AS ref_id, claim_type
FROM cleaned_claims
WHERE HOUR(reported_at) BETWEEN 6 AND 11
EXCEPT
SELECT id AS ref_id, status AS claim_type
FROM policies
ORDER BY ref_id, claim_type
