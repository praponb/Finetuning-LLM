SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range FROM order_items GROUP BY order_id ORDER BY order_id DESC, unit_price_range DESC LIMIT 50;
