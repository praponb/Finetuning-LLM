WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        UPPER(TRIM(status)) AS status_clean,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label
    FROM claims
    WHERE claim_date >= DATE '2024-06-27'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE claim_type IS NOT NULL) AS claim_claim_type_count,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        c.avg_claim_approved_amount
    FROM valid s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.status_clean,
        s.reported_at_label,
        s.claim_count,
        s.avg_claim_approved_amount,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status,
        po.region AS holder_region
    FROM with_stats s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
    WHERE p.holder_id >= 4
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(policy_status, '#', CAST(id AS STRING)) AS policy_status_ref,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date
    FROM combined_details_policy
    WHERE policy_holder_id IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    status_clean,
    reported_at_label,
    claim_count,
    avg_claim_approved_amount,
    policy_holder_id,
    policy_status,
    holder_region,
    LOWER(TRIM(holder_region)) AS holder_region_clean
FROM enriched
ORDER BY claim_count DESC, policy_holder_id
