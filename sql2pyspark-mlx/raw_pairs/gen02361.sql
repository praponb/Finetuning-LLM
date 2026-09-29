WITH with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.supplier_id,
        latest.inventory_quantity
    FROM products s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        price,
        stock_qty,
        launch_date,
        inventory_quantity,
        DATE_FORMAT(launch_date, 'yyyy-MM-dd') AS launch_date_label
    FROM with_last
    WHERE supplier_id IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        price,
        AVG(stock_qty) OVER (ORDER BY launch_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_2_prior,
        NTILE(10) OVER (ORDER BY inventory_quantity DESC, id) AS inventory_quantity_bucket
    FROM base
    WHERE launch_date > '2024-10-13'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.product_id IN (3, 4, 8, 10)
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(inventory_quantity_bucket, TRUE) OVER (ORDER BY stock_qty_avg_2_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_inventory_quantity_bucket,
        AVG(stock_qty_avg_2_prior) OVER (ORDER BY inventory_quantity_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS stock_qty_avg_2_prior_avg_2_prior
    FROM kept
)
-- Grouped metrics
SELECT
    CASE WHEN inventory_quantity_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS inventory_quantity_bucket_tier,
    CASE WHEN last_known_inventory_quantity_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS last_known_inventory_quantity_bucket_tier,
    ROUND(AVG(last_known_inventory_quantity_bucket), 2) AS avg_last_known_inventory_quantity_bucket,
    SUM(stock_qty_avg_2_prior) AS total_stock_qty_avg_2_prior,
    PERCENTILE_APPROX(stock_qty_avg_2_prior, 0.5) AS stock_qty_avg_2_prior_p50,
    MIN(stock_qty_avg_2_prior) AS min_stock_qty_avg_2_prior
FROM metrics
GROUP BY CASE WHEN inventory_quantity_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last_known_inventory_quantity_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END;
