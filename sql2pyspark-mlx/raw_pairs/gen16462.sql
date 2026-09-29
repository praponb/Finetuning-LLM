SELECT
    id,
    account_type,
    balance,
    customer_id
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS row_num
    FROM accounts
) AS r
WHERE row_num <= 2
