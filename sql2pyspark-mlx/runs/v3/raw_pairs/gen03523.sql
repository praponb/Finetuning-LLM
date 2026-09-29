WITH numbered AS (
    SELECT
        id,
        account_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS pos
    FROM transactions
    WHERE amount <= 1200
)
SELECT *
FROM numbered
WHERE pos = 1;
