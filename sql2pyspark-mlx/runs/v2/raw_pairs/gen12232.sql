WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) AS row_num
    FROM customers
    WHERE country = 'US'
)
SELECT *
FROM numbered
WHERE row_num <= 2
