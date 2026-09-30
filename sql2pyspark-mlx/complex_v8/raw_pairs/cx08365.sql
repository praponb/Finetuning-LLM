WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        FLOOR(premium / 1000) * 1000 AS premium_bucket_2
    FROM policies
    WHERE status IN ('EXPIRED', 'LAPSED')
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.product_or_unknown,
        s.premium_bucket_2,
        p.region AS holder_region,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score > 0.2
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        product_or_unknown,
        premium_bucket_2,
        holder_region,
        holder_segment,
        holder_risk_score,
        holder_risk_score - LAG(holder_risk_score) OVER (PARTITION BY product ORDER BY start_date NULLS LAST, id) AS holder_risk_score_change,
        SUM(holder_risk_score) OVER (PARTITION BY holder_region) AS holder_region_sum_holder_risk_score,
        ROUND(AVG(premium_bucket_2) OVER (PARTITION BY holder_region), 2) AS holder_region_avg_premium_bucket_2,
        COUNT(*) OVER (PARTITION BY holder_region) AS rows_in_holder_region,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER (PARTITION BY holder_region), 2) AS holder_risk_score_vs_holder_region_avg
    FROM enriched
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(reported_at) AS first_claim_reported_at,
        MAX(claimed_amount) AS max_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_reported_at,
        c.max_claim_claimed_amount
    FROM with_trends s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Subtotals with ROLLUP
SELECT
    holder_id,
    COALESCE(holder_region, 'ALL') AS holder_region,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(premium_bucket_2) / NULLIF(SUM(holder_risk_score_vs_holder_region_avg), 0), 3) AS premium_bucket_2_to_holder_risk_score_vs_holder_,
    ROUND(SUM(holder_risk_score_change) / NULLIF(SUM(holder_risk_score_vs_holder_region_avg), 0), 3) AS holder_risk_score_change_to_holder_risk_score_vs,
    PERCENTILE_APPROX(rows_in_holder_region, 0.5) AS median_rows_in_holder_region
FROM summarized_rows
GROUP BY ROLLUP(holder_id, holder_region)
