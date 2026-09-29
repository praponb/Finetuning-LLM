SELECT
    p.id,
    p.brand,
    t.avg_quantity,
    t.avg_unit_price
FROM products p
INNER JOIN (SELECT product_id, AVG(quantity) AS avg_quantity, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
