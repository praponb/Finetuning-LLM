WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claimed_amount + approved_amount AS claimed_amount_plus_approved_amount,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM claims
    WHERE status IN ('OPEN', 'PAID', 'APPROVED')
      AND claimed_amount BETWEEN 6745.1 AND 42447.2
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_plus_approved_amount,
        s.reported_at_plus_2h,
        p.product AS policy_product,
        p.status AS policy_status
    FROM base s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.product IN ('HOME', 'TRAVEL', 'HEALTH')
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        MIN(reported_at) AS first_claim_reported_at,
        MIN(approved_amount) AS min_claim_approved_amount,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        c.min_claim_approved_amount,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM with_parent_policy s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        COUNT(CASE WHEN claim_type = 'fire' THEN 1 END) AS claim_claim_type_count,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount_2
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_approved_amount,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        c.avg_claim_approved_amount_2
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
)
-- Subtotals with CUBE
SELECT
    adjuster_id,
    status,
    policy_id,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(policy_status)) AS policy_status_list
FROM profiled
GROUP BY CUBE(adjuster_id, status, policy_id)
ORDER BY adjuster_id NULLS LAST, status NULLS LAST, policy_id NULLS LAST
