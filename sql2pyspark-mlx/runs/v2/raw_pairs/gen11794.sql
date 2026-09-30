SELECT status, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
WHERE paid_at < '2023-08-01'
GROUP BY status
ORDER BY rnk DESC, status DESC;
