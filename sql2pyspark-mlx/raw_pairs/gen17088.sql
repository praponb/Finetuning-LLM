SELECT 'order' AS source, shipping_fee FROM orders WHERE store_id IN (9, 26) UNION SELECT 'store_sale' AS source, revenue FROM store_sales;
