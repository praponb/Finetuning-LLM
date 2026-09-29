WITH product_summary AS (
    SELECT product_id, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    p.supplier_id,
    ps.avg_unit_price
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id;
