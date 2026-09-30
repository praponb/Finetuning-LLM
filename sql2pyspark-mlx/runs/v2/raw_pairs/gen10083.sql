SELECT id, city, signup_date
FROM (
    SELECT
        id,
        city,
        signup_date,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY signup_date DESC) AS pos
    FROM customers
) AS ranked
WHERE pos <= 3
