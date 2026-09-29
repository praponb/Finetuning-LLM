SELECT *
FROM (
    SELECT
        id,
        city,
        signup_date,
        credit_limit,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY signup_date DESC) AS row_num
    FROM customers
) AS r
WHERE row_num <= 3
ORDER BY 2
