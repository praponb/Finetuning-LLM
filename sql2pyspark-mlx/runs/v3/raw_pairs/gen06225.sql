SELECT
    segment,
    city,
    SUM(CASE WHEN city = 'New York' THEN credit_limit ELSE 0 END) AS new_york_credit_limit,
    SUM(age) AS total_age
FROM customers
WHERE country IN ('TH', 'JP')
GROUP BY segment, city
HAVING COUNT(*) > 3
ORDER BY segment, city
LIMIT 20;
