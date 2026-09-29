SELECT status, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE method IN ('card', 'promptpay', 'cash')
GROUP BY status
HAVING stddev_amount < 32
ORDER BY status;
