SELECT order_id, COUNT(unit_price) AS unit_price_count, COUNT(quantity) AS quantity_count, COUNT(*) AS order_item_count FROM order_items GROUP BY order_id;
