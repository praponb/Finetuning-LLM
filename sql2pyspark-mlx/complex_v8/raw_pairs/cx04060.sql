WITH base AS (
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
        CASE WHEN approved_amount >= 16404.6 THEN 'large' WHEN approved_amount >= 0.0 THEN 'medium' ELSE 'small' END AS approved_amount_tier,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE adjuster_id <= 7
      AND reported_at >= TIMESTAMP '2025-01-25 14:00:00'
),
unique_rows AS (
    -- Keep the latest row per adjuster_id, status
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
        approved_amount_tier,
        claimed_amount_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, status ORDER BY claimed_amount DESC, id DESC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date
    FROM unique_rows s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.end_date < DATE '2026-12-05'
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.reported_at,
        s.policy_end_date,
        s.policy_start_date,
        p.premium AS policy_premium,
        p.status AS policy_status
    FROM with_parent_policy s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.premium < 4678.8
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    policy_end_date,
    policy_start_date
FROM combined_details
ORDER BY policy_start_date DESC
