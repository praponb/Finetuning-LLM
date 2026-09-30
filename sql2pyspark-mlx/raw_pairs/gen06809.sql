SELECT revenue, 'store_sale' AS origin FROM store_sales UNION ALL SELECT amount, 'order' AS origin FROM orders;
