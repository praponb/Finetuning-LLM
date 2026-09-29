SELECT revenue AS value, 'store_sale' AS source FROM store_sales UNION ALL SELECT amount AS value, 'order' AS source FROM orders;
