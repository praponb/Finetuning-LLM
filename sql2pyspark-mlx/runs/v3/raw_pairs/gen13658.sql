SELECT order_id, SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount FROM payments GROUP BY order_id HAVING SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END) >= 53;
