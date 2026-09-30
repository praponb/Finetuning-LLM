SELECT status, MIN(amount) AS min_amount, ROUND((100 * SUM(CASE WHEN method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_card FROM payments WHERE method = 'promptpay' GROUP BY status;
