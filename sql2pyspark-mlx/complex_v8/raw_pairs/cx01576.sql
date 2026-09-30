WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_parent_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.moved_at,
        s.operator,
        sk.category AS sku_category
    FROM scoped s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE sk.category <> 'Electronics'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY sku_category), 0), 4) AS qty_share_of_sku_category,
        RANK() OVER (ORDER BY qty DESC NULLS FIRST, id) AS qty_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY qty DESC NULLS FIRST), 3) AS qty_percent_rank,
        ROUND(qty - AVG(qty) OVER w_sku_category, 2) AS qty_vs_sku_category_avg,
        LAG(moved_at) OVER (ORDER BY qty DESC NULLS FIRST, id) AS prev_moved_at,
        SUM(qty) OVER (ORDER BY qty DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS qty_sum_3,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY qty ASC, id) AS qty_rank_2
    FROM with_parent_sku
    WINDOW w_sku_category AS (PARTITION BY sku_category)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE qty_rank <= 2
      AND moved_at < TIMESTAMP '2025-02-24 11:00:00'
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    operator,
    sku_category,
    qty_share_of_sku_category,
    qty_rank,
    qty_percent_rank,
    qty_vs_sku_category_avg,
    qty_sum_3,
    qty_rank_2,
    FLOOR(qty / 100) * 100 AS qty_bucket,
    CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
FROM top_per_group
ORDER BY sku_id DESC
