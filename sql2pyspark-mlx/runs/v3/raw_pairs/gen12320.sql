SELECT p.id, p.product_name, t.avg_unit_price FROM products p INNER JOIN (SELECT product_id, AVG(unit_price) AS avg_unit_price FROM order_items GROUP BY product_id) t ON p.id = t.product_id;
