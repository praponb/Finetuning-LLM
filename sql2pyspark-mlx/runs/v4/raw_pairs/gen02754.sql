SELECT
    id,
    segment,
    age,
    country,
    signup_date,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) row_num,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) rn
FROM customers
