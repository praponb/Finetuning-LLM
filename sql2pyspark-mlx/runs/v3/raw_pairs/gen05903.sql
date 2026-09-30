SELECT
    id,
    account_id,
    amount,
    merchant
FROM (
    SELECT
        id,
        account_id,
        amount,
        merchant,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS rn
    FROM transactions
) AS ranked
WHERE rn <= 3
ORDER BY amount
