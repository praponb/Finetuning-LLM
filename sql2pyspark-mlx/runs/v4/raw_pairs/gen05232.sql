WITH ranked AS (
    SELECT
        id,
        account_type,
        opened_date,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS rnk
    FROM accounts
)
SELECT
    id,
    account_type,
    opened_date,
    customer_id
FROM ranked
WHERE rnk <= 2
ORDER BY opened_date DESC;
