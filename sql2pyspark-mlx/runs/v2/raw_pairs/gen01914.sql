WITH ranked_accounts AS (
    SELECT
        id,
        status,
        balance,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) row_num
    FROM accounts
)
SELECT *
FROM ranked_accounts
WHERE row_num = 1
