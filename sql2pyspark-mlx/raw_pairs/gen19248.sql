WITH cleaned AS (
    SELECT *, ROUND(stock_qty * 100, 2) AS stock_qty_scaled
    FROM products
    WHERE supplier_id IS NOT NULL
),
enriched AS (
    SELECT
        src.id,
        src.product_name,
        src.cost,
        src.launch_date,
        src.tags,
        src.supplier_id,
        src.stock_qty_scaled,
        d.city,
        d.supplier_name
    FROM cleaned src
    LEFT JOIN suppliers d
        ON src.supplier_id = d.id
),
metrics AS (
    SELECT
        id,
        cost,
        launch_date,
        supplier_id,
        stock_qty_scaled,
        city,
        AVG(cost) OVER (ORDER BY cost ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3,
        AVG(stock_qty_scaled) OVER (PARTITION BY DATE_TRUNC('month', launch_date) ORDER BY launch_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS stock_qty_scaled_avg_2,
        LAST_VALUE(stock_qty_scaled) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY stock_qty_scaled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_stock_qty_scaled
    FROM enriched
    WHERE ARRAY_CONTAINS(tags, 'bestseller')
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.order_id IN (12, 14, 23, 33)
        )
),
base AS (
    SELECT
        id,
        launch_date,
        supplier_id,
        stock_qty_scaled,
        city,
        cost_avg_3,
        last_known_stock_qty_scaled,
        stock_qty_scaled / NULLIF(last_known_stock_qty_scaled, 0) AS stock_qty_scaled_per_last_known_stock_qty_scaled
    FROM kept
    WHERE launch_date > DATE '2023-05-10'
)
SELECT
    dim.city AS supplier_city,
    e.city AS product_city,
    COUNT(*) AS rows_total,
    MIN(e.last_known_stock_qty_scaled) AS min_last_known_stock_qty_scaled
FROM base e
INNER JOIN suppliers dim ON e.supplier_id = dim.id
GROUP BY GROUPING SETS ((dim.city, e.city), (dim.city), ());
