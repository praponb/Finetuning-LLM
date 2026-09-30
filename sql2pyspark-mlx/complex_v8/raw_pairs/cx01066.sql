WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE approved_amount > 0.0
),
with_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        p.holder_id AS policy_holder_id
    FROM base s
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_status, 0), 4) AS approved_amount_share_of_status,
        SUM(approved_amount) OVER (PARTITION BY status) AS status_sum_approved_amount,
        SIZE(COLLECT_SET(policy_holder_id) OVER (PARTITION BY status)) AS distinct_policy_holder_id_in_status,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status)) AS distinct_claim_type_in_status
    FROM with_details_policy
    WINDOW w_status AS (PARTITION BY status)
),
with_parent AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.policy_holder_id,
        s.approved_amount_share_of_status,
        s.status_sum_approved_amount,
        s.distinct_policy_holder_id_in_status,
        s.distinct_claim_type_in_status,
        p.segment AS holder_segment
    FROM sequenced s
    JOIN policyholders p ON p.id = s.policy_holder_id
    WHERE p.segment <> 'VIP'
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    policy_holder_id,
    approved_amount_share_of_status,
    holder_segment,
    DATE_TRUNC('YEAR', claim_date) AS claim_date_year,
    CASE WHEN claim_type IN ('theft', 'delay') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group
FROM with_parent
ORDER BY claim_type NULLS LAST, claim_date
