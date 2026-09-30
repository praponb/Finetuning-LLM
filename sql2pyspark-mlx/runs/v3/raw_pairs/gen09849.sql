SELECT product_id, ROUND(quantity, 2) AS quantity_rounded
FROM order_items
WHERE quantity != 5
ORDER BY 2 DESC, product_id
