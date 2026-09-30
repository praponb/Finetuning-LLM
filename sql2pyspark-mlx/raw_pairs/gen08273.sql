SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
GROUP BY product_id
HAVING MAX(quantity) - MIN(quantity) > 26;
