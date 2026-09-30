SELECT
    p.cost,
    p.category,
    t.avg_quantity,
    t.avg_unit_price
FROM products p
JOIN (SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity, AVG(unit_price) AS avg_unit_price
    FROM order_items
    WHERE unit_price BETWEEN 180 AND 390
    GROUP BY product_id) t ON p.id = t.product_id;
