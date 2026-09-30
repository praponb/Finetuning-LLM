SELECT order_id, COUNT(*) AS order_item_count, AVG(unit_price) AS avg_unit_price, SUM(quantity * unit_price) AS revenue FROM order_items GROUP BY order_id ORDER BY order_item_count DESC
