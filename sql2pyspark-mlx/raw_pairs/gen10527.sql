SELECT
    id,
    account_type,
    opened_date,
    status
FROM accounts
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) = 1
