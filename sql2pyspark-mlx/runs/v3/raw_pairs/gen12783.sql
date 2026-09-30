SELECT id, amount, RANK() OVER (ORDER BY amount DESC) AS rnk
FROM transactions
ORDER BY rnk DESC
