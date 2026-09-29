WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        CASE WHEN premium >= 3532.7 THEN 'HIGH' WHEN premium >= 2319.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE holder_id IS NOT NULL
),
filtered_2 AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IS NOT NULL
              AND p.start_date BETWEEN DATE '2024-03-15' AND DATE '2025-07-07'
        )
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_2 s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status IN ('APPROVED', 'IN_REVIEW', 'OPEN')
        )
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product_or_unknown,
        s.status_slug,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.reported_at AS claim_reported_at,
        c.claim_date
    FROM qualified s
    JOIN claims c ON c.policy_id = s.id
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product_or_unknown,
        s.status_slug,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_reported_at,
        p.region AS holder_region,
        c.claim_type,
        c.reported_at AS claim_reported_at_2
    FROM exploded_detail_claims s
    JOIN policyholders p ON p.id = s.holder_id
    JOIN claims c ON c.id = s.claim_id
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product_or_unknown,
        s.status_slug,
        s.claim_id,
        s.claim_adjuster_id,
        s.claim_reported_at,
        s.holder_region,
        s.claim_reported_at_2,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM combined_details s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        claim_id,
        claim_reported_at,
        holder_region,
        claim_reported_at_2,
        holder_joined_on,
        holder_risk_score,
        holder_segment,
        LENGTH(TRIM(product_or_unknown)) AS product_or_unknown_len
    FROM enriched_holder
    WHERE claim_reported_at < TIMESTAMP '2025-03-08 12:00:00'
)
SELECT
    id,
    claim_id,
    holder_region,
    claim_reported_at_2,
    holder_joined_on,
    holder_segment,
    product_or_unknown_len,
    CASE WHEN holder_segment IN ('VIP', 'RETAIL') THEN 'group_a' WHEN holder_segment = 'SME' THEN 'group_b' ELSE 'other' END AS holder_segment_group,
    CASE WHEN holder_risk_score >= 0.6 THEN 'large' WHEN holder_risk_score >= 0.3 THEN 'medium' ELSE 'small' END AS holder_risk_score_tier
FROM scored
ORDER BY product_or_unknown_len, claim_reported_at_2 DESC
