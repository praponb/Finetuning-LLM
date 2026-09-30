SELECT method, SUM(CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END) AS promptpay_count, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments WHERE order_id IN (19, 20) GROUP BY method
