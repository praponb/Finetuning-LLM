SELECT
    id,
    city,
    age,
    ROUND(age / SUM(age) OVER (PARTITION BY city), 3) AS age_share,
    RANK() OVER (PARTITION BY city ORDER BY age DESC) AS rnk
FROM customers
WHERE credit_limit >= 13400
ORDER BY age;
