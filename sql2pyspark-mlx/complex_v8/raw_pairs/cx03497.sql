WITH filtered AS (
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
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-04-09' AND DATE '2025-04-14'
),
status_stats AS (
    -- Max approved_amount per status
    SELECT status, MAX(approved_amount) AS max_approved_amount
    FROM filtered
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_approved_amount,
        ROUND(s.approved_amount - g.max_approved_amount, 2) AS approved_amount_minus_max
    FROM filtered s
    JOIN status_stats g ON g.status = s.status
    WHERE s.approved_amount = g.max_approved_amount
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        s.approved_amount_minus_max,
        p.product AS policy_product,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM outliers s
    JOIN policies p ON p.id = s.policy_id AND p.status = 'LAPSED'
    JOIN adjusters a ON a.id = s.adjuster_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    reported_at,
    approved_amount_minus_max,
    policy_product,
    adjuster_level,
    CASE WHEN claim_type IN ('theft', 'delay') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group,
    INITCAP(TRIM(policy_product)) AS policy_product_clean
FROM with_parent_policy
ORDER BY policy_id, policy_product DESC
