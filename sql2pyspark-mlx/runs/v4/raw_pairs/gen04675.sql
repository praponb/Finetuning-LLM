SELECT *
FROM (
    SELECT
        id,
        account_type,
        balance,
        status,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance DESC) AS rn
    FROM accounts
    WHERE customer_id = 6
) AS ranked
WHERE rn = 1
ORDER BY account_type DESC;
