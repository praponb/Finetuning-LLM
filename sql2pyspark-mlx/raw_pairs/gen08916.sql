SELECT
    city,
    is_active,
    COUNT(*) AS num_customers,
    SUM(age) AS total_age,
    AVG(age) AS avg_age
FROM customers
WHERE NOT is_active
GROUP BY city, is_active
ORDER BY total_age DESC, num_customers;
