WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date < s.launch_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.units > 14
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(cost) OVER w_part AS prev_cost,
        LAG(stock_qty) OVER w_part AS prev_stock_qty,
        ROW_NUMBER() OVER w_part AS row_seq,
        SUM(cost) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM eligible
    WINDOW w_part AS (PARTITION BY category ORDER BY stock_qty, id)
),
kept_keys AS (
    -- Set difference on keys
    SELECT category, supplier_id
    FROM metrics
    EXCEPT DISTINCT
    SELECT category, supplier_id
    FROM products
    WHERE cost > 520
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.category = k.category AND s.supplier_id = k.supplier_id
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.stock_qty,
    s.supplier_id,
    s.prev_cost,
    (SELECT AVG(t2.stock_qty) FROM products t2 WHERE t2.supplier_id = s.supplier_id) AS avg_product_stock_qty
FROM verified s
ORDER BY id DESC NULLS LAST
