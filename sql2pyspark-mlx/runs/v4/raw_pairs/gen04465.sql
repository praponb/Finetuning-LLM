SELECT
    id,
    city,
    age,
    country,
    signup_date,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS rn,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS row_num
FROM customers;
