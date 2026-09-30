SELECT
    p.id,
    p.price,
    p.category,
    t.avg_unit_price,
    t.num_order_items
FROM products p
INNER JOIN (SELECT product_id, ROUND(AVG(unit_price), 2) AS avg_unit_price, COUNT(*) AS num_order_items
    FROM order_items
    WHERE order_id IN (25, 38)
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY price ASC, category DESC, id ASC
LIMIT 3;
