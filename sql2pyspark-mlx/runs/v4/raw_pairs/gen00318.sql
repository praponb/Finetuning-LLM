SELECT method, SUM(amount) AS total_amount
FROM payments
GROUP BY method
HAVING SUM(amount) > 7400;
