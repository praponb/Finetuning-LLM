WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix
    FROM claims
    WHERE (claimed_amount > 12726.7 OR status = 'OPEN')
)
SELECT
    claim_type AS name,
    policy_id,
    reported_at AS happened_at,
    'claim' AS source
FROM claims
WHERE approved_amount < 33690.0
UNION
SELECT
    claim_type,
    policy_id,
    reported_at,
    'claim_2' AS source
FROM claims
