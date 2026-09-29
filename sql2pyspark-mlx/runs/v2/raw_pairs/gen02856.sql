SELECT method, SUM(amount) total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
WHERE method = 'bank_transfer'
GROUP BY method
