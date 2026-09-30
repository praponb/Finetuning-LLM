WITH kept_keys AS (
    -- Set intersection on keys
    SELECT id, product_id
    FROM order_items
    INTERSECT ALL
    SELECT id, product_id
    FROM order_items
    WHERE unit_price <= 300
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM order_items s
    JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.product_id = s.product_id) AS avg_store_sale_revenue
FROM kept_rows s
ORDER BY avg_store_sale_revenue DESC NULLS LAST
