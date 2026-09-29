SELECT
    p.id,
    p.category,
    t.quantity_count,
    t.avg_unit_price
FROM products p
INNER JOIN (SELECT product_id, COUNT(quantity) AS quantity_count, AVG(unit_price) AS avg_unit_price
    FROM order_items
    WHERE quantity BETWEEN 3 AND 6
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY quantity_count DESC
