SELECT country, is_active, SUM(CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END) AS singapore_count
FROM customers
WHERE signup_date BETWEEN '2023-05-01' AND '2023-05-03' AND city = 'Tokyo'
GROUP BY 1, 2
