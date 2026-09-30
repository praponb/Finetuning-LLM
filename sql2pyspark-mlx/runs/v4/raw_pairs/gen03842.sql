SELECT *
FROM (
    SELECT
        id,
        country,
        credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS row_num
    FROM customers
    WHERE country IN ('US', 'JP', 'TH')
) AS r
WHERE row_num <= 1
ORDER BY credit_limit DESC, id DESC
