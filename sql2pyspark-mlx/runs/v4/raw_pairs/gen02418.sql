SELECT status, SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount
FROM payments
GROUP BY status
HAVING SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) >= 74
ORDER BY status ASC, success_amount DESC;
