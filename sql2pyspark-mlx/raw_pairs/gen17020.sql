SELECT status, SUM(CASE WHEN method = 'promptpay' THEN 1 ELSE 0 END) AS promptpay_count, ROUND(AVG(amount), 2) AS avg_amount FROM payments GROUP BY status;
