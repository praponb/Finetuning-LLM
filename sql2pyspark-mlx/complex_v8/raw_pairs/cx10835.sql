WITH recent_claims AS (
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
        DAYOFWEEK(reported_at) AS reported_at_dayofweek
    FROM claims
    WHERE status NOT IN ('REJECTED', 'APPROVED')
)
SELECT 'claim' AS origin, reported_at_dayofweek, status AS label
FROM recent_claims
UNION ALL
SELECT 'holder' AS origin, id, segment
FROM policyholders
ORDER BY label DESC, origin
