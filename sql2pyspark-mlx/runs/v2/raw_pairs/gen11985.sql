SELECT
    id,
    account_type,
    opened_date,
    customer_id
FROM (
    SELECT
        id,
        account_type,
        opened_date,
        customer_id,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY opened_date) AS rn
    FROM accounts
    WHERE account_type = 'CREDIT'
) AS t
WHERE rn = 1
ORDER BY id, 3;
