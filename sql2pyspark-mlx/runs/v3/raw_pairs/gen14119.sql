SELECT order_id, COUNT(*) order_item_count FROM order_items GROUP BY order_id ORDER BY order_id ASC LIMIT 1;
