SELECT
    id,
    account_type,
    balance,
    status,
    opened_date,
    FIRST_VALUE(status) OVER (PARTITION BY account_type ORDER BY balance, id) AS first_status,
    RANK() OVER (PARTITION BY status ORDER BY opened_date) AS rnk
FROM accounts
