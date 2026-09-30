SELECT city, SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END) jp_count, AVG(credit_limit) avg_credit_limit
FROM customers
GROUP BY 1
ORDER BY avg_credit_limit DESC;
