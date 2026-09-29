WITH base_claims AS (
    -- Filter claims
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
        CASE WHEN claimed_amount >= 29359.4 THEN 'HIGH' WHEN claimed_amount >= 11235.6 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier,
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE status = 'OPEN'
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_tier,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_date AS claim_claim_date,
        c.claim_type AS claim_claim_type
    FROM base_claims s
    JOIN claims c ON c.policy_id = s.policy_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('YEAR', claim_claim_date) AS claim_claim_date_year,
        SUBSTRING(TRIM(claim_claim_type), 1, 2) AS claim_claim_type_prefix,
        ROUND(approved_amount / NULLIF(claim_approved_amount, 0), 3) AS approved_amount_per_claim_approved_amount
    FROM exploded_detail
)
SELECT
    id,
    policy_id,
    claim_type,
    approved_amount,
    reported_at,
    claimed_amount_tier,
    claim_id,
    claim_approved_amount,
    claim_claim_date,
    ADD_MONTHS(claim_claim_date, 1) AS claim_claim_date_plus_1m,
    SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
FROM annotated
