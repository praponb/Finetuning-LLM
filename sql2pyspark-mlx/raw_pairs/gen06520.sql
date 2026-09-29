SELECT COUNT(DISTINCT quantity) AS unique_orders, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items;
