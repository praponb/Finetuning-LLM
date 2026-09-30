SELECT
    city,
    country,
    COUNT(age) AS age_count,
    SUM(age) AS total_age,
    COUNT(*) AS row_count
FROM customers
WHERE city IS NULL
GROUP BY city, country;
