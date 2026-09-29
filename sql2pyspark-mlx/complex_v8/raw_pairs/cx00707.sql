WITH recent_claims AS (
    -- Scope the claims rows
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
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT
    adjuster_id AS qty,
    reported_at,
    claimed_amount,
    'claim' AS record_type
FROM recent_claims
UNION ALL
SELECT
    id,
    reported_at,
    approved_amount,
    'claim_2' AS record_type
FROM claims
