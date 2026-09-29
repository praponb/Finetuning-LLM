WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_num
    FROM customers
)
SELECT *
FROM numbered
WHERE row_num <= 2
