WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE (premium < 2945.9 OR end_date >= DATE '2025-10-12')
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        COUNT(CASE WHEN claim_type IN ('medical', 'delay') THEN 1 END) AS claim_claim_type_count,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_approved_amount,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM prepared_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(claim_claim_type_count) OVER (PARTITION BY product) AS product_min_claim_claim_type_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY end_date DESC, id), 3) AS end_date_percent_rank,
        ROUND(avg_claim_approved_amount - AVG(avg_claim_approved_amount) OVER w_product, 2) AS avg_claim_approved_amount_vs_product_avg
    FROM summarized_rows
    WINDOW w_product AS (PARTITION BY product)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, claim_count, product_min_claim_claim_type_count, claim_claim_type_count
    FROM metrics
) src
UNPIVOT (
    metric_value FOR measure IN (claim_count, product_min_claim_claim_type_count, claim_claim_type_count)
) unp
