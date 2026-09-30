SELECT *, ROUND(quantity, 1) AS quantity_rounded, ROUND(quantity * 1.07, 2) AS quantity_scaled
FROM order_items
WHERE product_id IS NULL
ORDER BY quantity_scaled DESC;
