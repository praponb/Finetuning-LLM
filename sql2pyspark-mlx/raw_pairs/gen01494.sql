SELECT order_id, MAX(quantity) - MIN(quantity) quantity_range
FROM order_items
WHERE product_id IS NOT NULL OR quantity IS NOT NULL
GROUP BY order_id
HAVING MIN(quantity) > 7.0
ORDER BY quantity_range;
