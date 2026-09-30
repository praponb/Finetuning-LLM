WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS row_num
    FROM customers
    WHERE UPPER(TRIM(email)) = 'EMMA@OUTLOOK.COM'
)
SELECT *
FROM numbered
WHERE row_num <= 5
ORDER BY credit_limit DESC
