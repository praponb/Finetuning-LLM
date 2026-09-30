WITH product_summary AS (
    SELECT product_id, AVG(quantity) AS avg_quantity
    FROM inventory
    GROUP BY product_id
)
SELECT p.supplier_id, p.stock_qty, ps.avg_quantity
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.avg_quantity >= 110
ORDER BY ps.avg_quantity;
