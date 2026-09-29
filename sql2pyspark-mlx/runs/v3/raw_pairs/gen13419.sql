SELECT
    id,
    city,
    segment,
    age,
    NTILE(2) OVER (PARTITION BY city, segment ORDER BY age, id) AS bucket
FROM customers
WHERE MONTH(signup_date) IN (1, 2, 3);
