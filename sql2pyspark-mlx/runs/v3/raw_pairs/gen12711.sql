SELECT
    id,
    city,
    signup_date,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS rn
FROM customers;
