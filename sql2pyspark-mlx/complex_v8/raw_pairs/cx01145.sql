WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE status = 'APPROVED'
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        p.status AS policy_status,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id
    FROM cleaned_claims s
    JOIN policies p ON p.id = s.policy_id AND p.product IN ('TRAVEL', 'LIFE')
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.policy_status,
        s.policy_product,
        s.policy_holder_id,
        p.premium AS policy_premium
    FROM combined_details_policy s
    JOIN policies p ON p.id = s.policy_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        GREATEST(policy_premium, claimed_amount) AS greatest_policy_premium_claimed_amount
    FROM with_parent
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.status,
        s.policy_status,
        s.policy_product,
        s.policy_holder_id,
        s.policy_premium,
        s.status_slug,
        s.greatest_policy_premium_claimed_amount,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM tagged s
    JOIN policyholders p ON p.id = s.policy_holder_id
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.policy_status,
        s.policy_holder_id,
        s.policy_premium,
        s.status_slug,
        s.holder_segment,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.status AS claim_status
    FROM enriched s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    policy_holder_id,
    claim_id,
    claim_reported_at,
    claim_status,
    CASE WHEN policy_premium >= 3544.1 THEN 'large' WHEN policy_premium >= 2013.8 THEN 'medium' ELSE 'small' END AS policy_premium_tier
FROM detail_rows_claims
ORDER BY adjuster_id NULLS LAST
