WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        DAYOFWEEK(end_date) AS end_date_dayofweek,
        CASE WHEN product IN ('HEALTH', 'TRAVEL') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
        CASE WHEN status IN ('EXPIRED', 'CANCELLED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE status = 'ACTIVE'
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.product_group,
        s.status_group,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.holder_name
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score < 0.8
),
with_parent AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.product_group,
        s.status_group,
        s.holder_segment,
        s.holder_region,
        s.holder_name,
        p.region AS holder_region_2,
        p.risk_score AS holder_risk_score
    FROM joined s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS first_claim_date,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_approved_amount
    FROM with_parent s
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status_group,
        s.holder_region_2,
        s.holder_risk_score,
        s.claim_count,
        s.first_claim_date,
        s.total_claim_approved_amount,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_type,
        c.claim_date
    FROM with_stats s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount < 31715.7
),
with_details AS (
    -- Join claims
    SELECT
        s.id,
        s.holder_id,
        s.status_group,
        s.holder_region_2,
        s.claim_count,
        s.first_claim_date,
        s.total_claim_approved_amount,
        s.claim_id,
        s.claim_status,
        c.claim_date AS claim_claim_date,
        c.reported_at AS claim_reported_at,
        p.segment AS holder_segment
    FROM exploded_detail s
    LEFT JOIN claims c ON c.id = s.claim_id
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IN ('ACTIVE', 'EXPIRED')
              AND p.start_date >= DATE '2025-01-05'
        )
)
-- Final projection
SELECT
    id,
    status_group,
    holder_region_2,
    claim_count,
    claim_status,
    claim_claim_date,
    claim_reported_at,
    holder_segment
FROM screened
WHERE claim_claim_date < DATE '2024-12-20'
ORDER BY claim_status DESC
