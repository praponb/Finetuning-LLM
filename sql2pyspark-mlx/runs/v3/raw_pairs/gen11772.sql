SELECT order_id, unique_orders FROM (SELECT order_id, COUNT(DISTINCT quantity) AS unique_orders FROM order_items GROUP BY order_id) AS agg WHERE unique_orders > 7;
