SELECT segment, SUM(credit_limit) AS total_credit_limit, DENSE_RANK() OVER (ORDER BY SUM(credit_limit) DESC) AS rnk
FROM customers
GROUP BY segment;
