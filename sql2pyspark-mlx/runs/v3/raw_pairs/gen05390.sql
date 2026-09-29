SELECT order_id, product_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY order_id, product_id;
