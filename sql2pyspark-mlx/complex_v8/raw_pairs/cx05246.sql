WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-05-30'
      AND end_date IS NOT NULL
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        holder_segment,
        holder_name,
        CASE WHEN holder_risk_score >= 0.6 THEN 'A' WHEN holder_risk_score >= 0.2 THEN 'B' ELSE 'C' END AS holder_risk_score_tier,
        CASE WHEN holder_segment IN ('SME', 'VIP') THEN 'group_a' WHEN holder_segment = 'RETAIL' THEN 'group_b' ELSE 'other' END AS holder_segment_group
    FROM with_parent_holder
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(TRIM(holder_name)) AS holder_name_clean,
        SUBSTRING(TRIM(holder_name), 1, 2) AS holder_name_prefix
    FROM derived
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(approved_amount) AS total_claim_approved_amount,
        MAX(reported_at) AS last_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount,
        c.last_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM enriched s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, holder_segment_group, total_claim_approved_amount
    FROM profiled
) src
PIVOT (
    MIN(total_claim_approved_amount)
    FOR holder_segment_group IN ('group_b', 'other', 'group_a')
)
