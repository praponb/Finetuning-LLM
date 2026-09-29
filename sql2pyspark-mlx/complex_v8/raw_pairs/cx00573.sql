WITH recent AS (
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
        reported_at
    FROM claims
    WHERE status IN ('OPEN', 'IN_REVIEW', 'APPROVED')
)
SELECT
    'claim' AS record_type,
    adjuster_id,
    claimed_amount,
    claim_date AS event_date
FROM recent
WHERE MONTH(reported_at) IN (1, 3)
  AND claimed_amount <= 34401.0
UNION ALL
SELECT
    'policy' AS record_type,
    holder_id AS adjuster_id,
    premium AS claimed_amount,
    end_date AS event_date
FROM policies
UNION ALL
SELECT
    'holder' AS record_type,
    id AS adjuster_id,
    risk_score AS claimed_amount,
    joined_on AS event_date
FROM policyholders
ORDER BY claimed_amount, adjuster_id
