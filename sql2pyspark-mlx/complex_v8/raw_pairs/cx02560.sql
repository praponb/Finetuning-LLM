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
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix,
        TO_DATE(reported_at) AS reported_at_date,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        reported_at,
        claim_type_prefix,
        claimed_amount_bucket,
        ABS(approved_amount - 2349.3) AS approved_amount_distance,
        QUARTER(reported_at_date) AS reported_at_date_quarter
    FROM recent_claims
    WHERE claimed_amount BETWEEN 14019.8 AND 32878.5
)
SELECT
    id,
    claim_type_prefix,
    approved_amount_distance,
    reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d
FROM annotated
WHERE approved_amount_distance >= 17500.0
ORDER BY approved_amount_distance DESC NULLS FIRST, id
LIMIT 20
