SELECT revenue, 'store_sale' AS record_type FROM store_sales UNION SELECT shipping_fee AS revenue, 'order' AS record_type FROM orders WHERE amount <= 1280 ORDER BY revenue;
