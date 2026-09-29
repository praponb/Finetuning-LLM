SELECT p.product_name, t.avg_unit_price
FROM products p
INNER JOIN (SELECT product_id, ROUND(AVG(unit_price), 2) avg_unit_price
    FROM order_items
    WHERE unit_price < 270
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY t.avg_unit_price DESC;
