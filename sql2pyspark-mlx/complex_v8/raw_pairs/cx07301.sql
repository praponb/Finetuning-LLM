WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
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
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT id AS qty, claim_date, reported_at AS happened_at
FROM valid_claims
WHERE claim_type IN ('theft', 'delay')
  AND claim_type_prefix IS NOT NULL
UNION ALL
SELECT id AS qty, claim_date, reported_at AS happened_at
FROM claims
WHERE approved_amount > 0.0
