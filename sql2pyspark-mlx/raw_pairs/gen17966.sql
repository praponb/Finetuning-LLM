SELECT id, account_id, amount
FROM (
    SELECT
        id,
        account_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS rn
    FROM transactions
) AS t
WHERE rn <= 3
ORDER BY 3 DESC, id DESC
