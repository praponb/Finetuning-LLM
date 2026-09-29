WITH top_accounts AS (
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) AS row_num
    FROM accounts
    WHERE status NOT IN ('CLOSED', 'FROZEN', 'OPEN')
)
SELECT
    id,
    customer_id,
    balance,
    opened_date
FROM top_accounts
WHERE row_num <= 1
