WITH product_summary AS (
    SELECT product_id, COUNT(quantity) AS quantity_count
    FROM order_items
    WHERE product_id IS NULL
    GROUP BY product_id
)
SELECT p.brand, p.category, ps.quantity_count
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
ORDER BY brand
