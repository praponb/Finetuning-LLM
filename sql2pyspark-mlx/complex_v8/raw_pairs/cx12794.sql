WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
)
SELECT
    'claim' AS record_type,
    id,
    approved_amount,
    claim_type AS label
FROM cleaned_claims
WHERE adjuster_id IS NOT NULL
  AND YEAR(claim_date) = 2025
UNION ALL
SELECT
    'claim_2' AS record_type,
    adjuster_id,
    claimed_amount,
    status
FROM claims
