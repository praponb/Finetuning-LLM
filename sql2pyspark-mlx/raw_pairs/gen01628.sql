WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY price) AS price_dense_rank,
        LEAD(cost, 1, 0) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS next_cost,
        STDDEV_SAMP(stock_qty) OVER (PARTITION BY category, supplier_id) AS category_stddev_samp_stock_qty,
        SUM(price) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_price,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), brand ORDER BY stock_qty DESC, id) AS event_seq
    FROM products
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        next_cost,
        running_price,
        CAST(category_stddev_samp_stock_qty AS DOUBLE) AS category_stddev_samp_stock_qty_num
    FROM enriched
    WHERE launch_date >= DATE '2023-08-31'
),
base_products AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        tags,
        supplier_id,
        running_price,
        category_stddev_samp_stock_qty_num,
        ROUND(category_stddev_samp_stock_qty_num * 1.15, 2) AS category_stddev_samp_stock_qty_num_scaled
    FROM base
    WHERE stock_qty = 338
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_products s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.product_id IN (6, 33, 37)
        )
)
-- Grouped metrics
SELECT
    supplier_id,
    CASE WHEN category_stddev_samp_stock_qty_num_scaled >= 68 THEN 'LARGE' ELSE 'SMALL' END AS category_stddev_samp_stock_qty_num_scaled_tier,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    CORR(price, stock_qty) AS corr_price_stock_qty,
    GROUPING_ID() AS gid
FROM eligible
GROUP BY ROLLUP (supplier_id, CASE WHEN category_stddev_samp_stock_qty_num_scaled >= 68 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY corr_price_stock_qty
