SELECT order_id, COUNT(unit_price) AS unit_price_count, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY order_id;
