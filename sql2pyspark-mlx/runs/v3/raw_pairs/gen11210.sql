SELECT order_id, MAX(unit_price) - MIN(unit_price) unit_price_range, COUNT(DISTINCT unit_price) unique_products FROM order_items GROUP BY order_id ORDER BY order_id;
