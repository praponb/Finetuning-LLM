WITH product_totals AS (
    SELECT product_id, SUM(quantity) AS total_quantity
    FROM inventory
    WHERE product_id = 5
    GROUP BY product_id
)
SELECT p.category, pt.total_quantity
FROM products p
LEFT JOIN product_totals pt ON p.id = pt.product_id
ORDER BY pt.total_quantity, category;
