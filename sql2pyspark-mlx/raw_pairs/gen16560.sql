WITH product_summary AS (
    SELECT product_id, MIN(quantity) AS min_quantity
    FROM order_items
    WHERE product_id = 35
    GROUP BY product_id
)
SELECT p.category, p.price, ps.min_quantity
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.min_quantity >= 8.1
ORDER BY p.category, min_quantity;
