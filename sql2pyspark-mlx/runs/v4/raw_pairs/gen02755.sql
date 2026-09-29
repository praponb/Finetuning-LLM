SELECT order_id, unit_price_count FROM (SELECT order_id, COUNT(unit_price) AS unit_price_count FROM order_items GROUP BY order_id) AS s WHERE unit_price_count >= 4 ORDER BY order_id DESC LIMIT 20;
