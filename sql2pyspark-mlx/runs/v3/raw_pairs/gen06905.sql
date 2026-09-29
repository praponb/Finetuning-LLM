SELECT order_date, 'order' AS source FROM orders WHERE customer_id = 25 UNION ALL SELECT opened_date, 'store' AS source FROM stores;
