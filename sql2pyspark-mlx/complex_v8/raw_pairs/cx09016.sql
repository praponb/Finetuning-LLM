WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        CASE WHEN approved_amount >= 23487.5 THEN 'large' WHEN approved_amount >= 9389.3 THEN 'medium' ELSE 'small' END AS approved_amount_tier
    FROM claims
    WHERE claimed_amount <= 39666.5
      AND TO_DATE(reported_at) < DATE '2025-03-08'
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.claim_type_prefix,
        s.approved_amount_tier,
        p.status AS policy_status,
        p.start_date AS policy_start_date,
        po.joined_on AS holder_joined_on,
        po.segment AS holder_segment
    FROM valid s
    LEFT JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.claim_type_prefix,
        s.approved_amount_tier,
        s.policy_status,
        s.policy_start_date,
        s.holder_joined_on,
        s.holder_segment,
        c.id AS claim_id,
        c.claim_type,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount
    FROM combined_details_policy s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.claim_type_prefix,
        s.approved_amount_tier,
        s.policy_status,
        s.policy_start_date,
        s.holder_joined_on,
        s.holder_segment,
        s.claim_id,
        s.claim_type,
        s.claim_reported_at,
        s.claim_claimed_amount,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date,
        c.status AS claim_status
    FROM line_level_claims s
    LEFT JOIN claims c ON c.id = s.claim_id AND c.status IN ('PAID', 'REJECTED', 'APPROVED')
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM joined
    WHERE claim_claimed_amount > (SELECT MIN(claim_claimed_amount) FROM joined)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    claim_type_prefix,
    approved_amount_tier,
    policy_status,
    policy_start_date,
    holder_joined_on,
    claim_id,
    claim_type,
    claim_reported_at,
    claim_claimed_amount,
    claim_adjuster_id,
    claim_date,
    claim_status,
    claim_reported_at + INTERVAL 1 DAYS AS claim_reported_at_plus_1d
FROM with_activity
WHERE holder_joined_on IS NOT NULL
