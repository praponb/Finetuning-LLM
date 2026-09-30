WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d
    FROM claims
    WHERE status NOT IN ('APPROVED', 'OPEN')
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        claim_date_plus_365d,
        DATEDIFF(claim_date, claim_date_plus_365d) AS days_claim_date_plus_365d_to_claim_date
    FROM prepared_claims
    WHERE claim_date_plus_365d >= DATE '2024-07-01'
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN claimed_amount >= 31715.7 THEN 'HIGH' WHEN claimed_amount >= 10518.4 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier
    FROM tagged
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_type,
    claimed_amount,
    claim_date_plus_365d,
    claimed_amount_tier,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM tagged_2
