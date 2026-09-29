SELECT
    id,
    status,
    account_type,
    opened_date,
    LAG(opened_date, 1) OVER (PARTITION BY status, account_type ORDER BY opened_date, id) AS prev_opened_date
FROM accounts
