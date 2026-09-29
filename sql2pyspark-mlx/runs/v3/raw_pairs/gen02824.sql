SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount, COUNT(CASE WHEN method = 'paypal' THEN 1 END) AS paypal_count FROM payments WHERE amount BETWEEN 920 AND 990 GROUP BY order_id;
