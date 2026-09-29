SELECT method, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
GROUP BY method
ORDER BY 3;
