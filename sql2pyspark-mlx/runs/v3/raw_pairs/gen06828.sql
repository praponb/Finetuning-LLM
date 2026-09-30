SELECT *
FROM (
    SELECT
        id,
        customer_id,
        balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) row_num
    FROM accounts
) ranked
WHERE row_num <= 5
ORDER BY 3 DESC
