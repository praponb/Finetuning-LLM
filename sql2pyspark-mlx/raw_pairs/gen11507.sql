SELECT opened_date, 'store' AS source FROM stores UNION ALL SELECT order_date, 'order' AS source FROM orders WHERE status IN ('RETURNED', 'COMPLETED')
