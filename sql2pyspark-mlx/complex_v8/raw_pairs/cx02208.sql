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
        reported_at,
        LOWER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 10 AND 12
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        approved_amount,
        status,
        reported_at,
        claim_type_clean,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM recent
)
-- Final projection
SELECT
    id,
    policy_id,
    approved_amount,
    reported_at,
    claim_type_clean,
    reported_at_plus_2h
FROM tagged
ORDER BY reported_at_plus_2h NULLS LAST, claim_type_clean DESC
