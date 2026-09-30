SELECT
    id,
    city,
    signup_date,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY signup_date) AS dense_rnk
FROM customers
WHERE name LIKE 'Iv%'
ORDER BY id DESC;
