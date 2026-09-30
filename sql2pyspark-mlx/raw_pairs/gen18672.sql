WITH product_totals AS (
    SELECT product_id, SUM(quantity) AS total_quantity, COUNT(quantity) AS quantity_count
    FROM inventory
    GROUP BY product_id
)
SELECT
    p.id,
    p.product_name,
    pt.total_quantity,
    pt.quantity_count
FROM products p
JOIN product_totals pt ON p.id = pt.product_id
WHERE pt.total_quantity > 940
