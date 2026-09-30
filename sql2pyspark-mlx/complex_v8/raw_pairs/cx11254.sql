WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ROUND(premium * 1.1, 2) AS premium_adj,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date,
        ABS(premium - 2824.5) AS premium_distance
    FROM policies
    WHERE status IN ('ACTIVE', 'LAPSED')
      AND premium <= 3817.5
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        COUNT(CASE WHEN claim_type = 'theft' THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM base s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        premium_adj,
        days_since_start_date,
        claim_count,
        claim_claim_type_count,
        CASE WHEN days_since_start_date >= 490 THEN 'A' WHEN days_since_start_date >= 175 THEN 'B' ELSE 'C' END AS days_since_start_date_tier
    FROM with_counts
    WHERE end_date < DATE '2026-09-19'
)
-- Subtotals with CUBE
SELECT
    COALESCE(product, 'ALL') AS product,
    claim_count,
    claim_claim_type_count,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt
FROM annotated
GROUP BY CUBE(product, claim_count, claim_claim_type_count)
ORDER BY product NULLS LAST, claim_count NULLS LAST, claim_claim_type_count NULLS LAST
