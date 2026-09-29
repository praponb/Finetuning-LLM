WITH product_totals AS (
    SELECT product_id, COUNT(quantity) AS quantity_count, MAX(last_restock) AS latest_last_restock
    FROM inventory
    GROUP BY product_id
)
SELECT p.product_name, pt.quantity_count, pt.latest_last_restock
FROM products p
JOIN product_totals pt ON p.id = pt.product_id
ORDER BY quantity_count DESC, p.product_name
