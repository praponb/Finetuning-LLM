WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 0.25, 2) AS premium_adj
    FROM policies
    WHERE holder_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        premium_adj,
        ROUND(premium_adj - AVG(premium_adj) OVER (PARTITION BY status), 2) AS premium_adj_vs_status_avg,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status)) AS distinct_product_in_status
    FROM filtered
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM sequenced s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_date BETWEEN DATE '2024-05-14' AND DATE '2025-03-31'
        )
      AND s.product = 'AUTO'
)
-- Subtotals with GROUPING SETS
SELECT
    product,
    holder_id,
    ROUND(100 * COUNT(CASE WHEN product <> 'HEALTH' THEN 1 END) / COUNT(*), 1) AS pct_product_health,
    MIN(end_date) AS first_end_date,
    SUM(CASE WHEN product = 'LIFE' THEN 1 ELSE 0 END) AS n_product_life
FROM with_activity
GROUP BY GROUPING SETS ((product, holder_id), (product), (holder_id))
ORDER BY product NULLS LAST, holder_id NULLS LAST
