WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        CASE WHEN claimed_amount >= 41878.5 THEN 'HIGH' WHEN claimed_amount >= 13898.1 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier
    FROM claims
    WHERE claimed_amount < 43724.9
),
status_stats AS (
    -- Median approved_amount per status
    SELECT status, PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount
    FROM filtered_claims
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_approved_amount,
        ROUND(s.approved_amount - g.median_approved_amount, 2) AS approved_amount_minus_median
    FROM filtered_claims s
    JOIN status_stats g ON g.status = s.status
    WHERE s.approved_amount >= g.median_approved_amount
),
with_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.claimed_amount_tier,
        s.median_approved_amount,
        s.approved_amount_minus_median,
        p.premium AS policy_premium,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        po.segment AS holder_segment,
        po.risk_score AS holder_risk_score
    FROM outliers s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount_tier,
        s.median_approved_amount,
        s.approved_amount_minus_median,
        s.policy_premium,
        s.policy_status,
        s.policy_end_date,
        s.holder_risk_score,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date_2
    FROM with_details s
    JOIN policies p ON p.id = s.policy_id AND p.status IS NOT NULL
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount_tier,
    policy_premium,
    policy_status,
    policy_end_date,
    policy_end_date_2,
    ROUND(median_approved_amount / NULLIF(holder_risk_score, 0), 3) AS median_approved_amount_per_holder_risk_score
FROM with_details_policy
ORDER BY policy_id DESC, policy_end_date_2, id
LIMIT 5
