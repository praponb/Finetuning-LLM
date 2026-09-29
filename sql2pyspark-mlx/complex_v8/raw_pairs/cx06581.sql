WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE MONTH(reported_at) IN (1, 2)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount_2
    FROM filtered
),
labeled_2 AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM labeled
    WHERE claim_date IS NOT NULL
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.claimed_amount_per_approved_amount_2,
        s.status_slug,
        s.status_prefix,
        c.id AS claim_id,
        c.status AS claim_status,
        c.policy_id AS claim_policy_id,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at
    FROM labeled_2 s
    JOIN claims c ON c.adjuster_id = s.adjuster_id
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.status,
        s.claimed_amount_per_approved_amount_2,
        s.status_slug,
        s.status_prefix,
        s.claim_id,
        s.claim_status,
        s.claim_policy_id,
        s.claim_claim_date,
        s.claim_reported_at,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium,
        p.product AS policy_product
    FROM exploded_detail s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.holder_id <= 11
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount_per_approved_amount_2,
    status_slug,
    status_prefix,
    claim_id,
    claim_status,
    claim_policy_id,
    claim_claim_date,
    claim_reported_at,
    policy_premium,
    policy_product,
    DATE_TRUNC('MONTH', claim_reported_at) AS claim_reported_at_month,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug_2
FROM enriched
ORDER BY policy_id NULLS LAST, status_slug NULLS LAST, id, claim_id
LIMIT 5
