SELECT segment, COUNT(age) AS age_count, COUNT(credit_limit) AS credit_limit_count
FROM customers
GROUP BY segment;
