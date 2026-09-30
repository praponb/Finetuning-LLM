WITH numbered AS (
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) pos
    FROM accounts
)
SELECT *
FROM numbered
WHERE pos <= 5;
