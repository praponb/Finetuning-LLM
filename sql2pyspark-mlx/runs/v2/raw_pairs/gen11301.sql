SELECT status, ROUND(AVG(amount), 2) avg_amount, SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) success_amount
FROM payments
GROUP BY 1
ORDER BY avg_amount DESC, status
LIMIT 10;
