SELECT
    id,
    segment,
    credit_limit,
    signup_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS row_num
    FROM customers
    WHERE segment IS NOT NULL
) AS r
WHERE row_num <= 5
ORDER BY 1 DESC, credit_limit ASC;
