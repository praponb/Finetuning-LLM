WITH ranked_accounts AS (
    SELECT
        id,
        account_type,
        opened_date,
        balance,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS row_num
    FROM accounts
)
SELECT *
FROM ranked_accounts
WHERE row_num <= 3;
