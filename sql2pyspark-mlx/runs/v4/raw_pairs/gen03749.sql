WITH valid AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date,
        COALESCE(brand, 'UNKNOWN') AS brand_clean
    FROM products
    WHERE launch_date > DATE '2023-06-22'
),
remaining_keys AS (
    SELECT id
    FROM valid
    EXCEPT
    SELECT id
    FROM products
    WHERE price <= 640
),
reconciled AS (
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    SELECT
        *,
        price / NULLIF(stock_qty, 0) AS price_per_stock_qty,
        CAST(stock_qty AS DOUBLE) AS stock_qty_num
    FROM reconciled
    WHERE launch_date >= DATE '2023-09-04'
      AND supplier_id IS NOT NULL
      AND stock_qty BETWEEN 189 AND 233
),
scored AS (
    SELECT
        *,
        MAX(price) OVER (PARTITION BY brand) AS brand_max_price,
        DENSE_RANK() OVER w_ordered AS days_since_launch_date_dense_rank,
        RANK() OVER (PARTITION BY category ORDER BY stock_qty_num) AS stock_qty_num_rank,
        RANK() OVER w_part AS price_per_stock_qty_rank
    FROM base
    WINDOW w_ordered AS (PARTITION BY supplier_id ORDER BY days_since_launch_date NULLS FIRST), w_part AS (ORDER BY price_per_stock_qty NULLS FIRST)
)
SELECT *
FROM (
    SELECT
        category,
        price,
        stock_qty_num
    FROM scored
) src
UNPIVOT (
    metric_value FOR metric_name IN (price, stock_qty_num)
);
