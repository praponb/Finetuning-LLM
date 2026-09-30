SELECT amount, 'order' AS source FROM orders WHERE channel IS NOT NULL UNION SELECT revenue AS amount, 'store_sale' AS source FROM store_sales;
