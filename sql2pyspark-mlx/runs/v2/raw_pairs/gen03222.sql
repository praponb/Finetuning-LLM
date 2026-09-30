SELECT product_id
FROM order_items
WHERE product_id IS NOT NULL AND (quantity < 8 OR order_id IN (5, 15, 25, 37))
ORDER BY product_id DESC;
