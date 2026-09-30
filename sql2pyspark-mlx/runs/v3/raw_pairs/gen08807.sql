SELECT
    id,
    customer_id,
    opened_date,
    account_type,
    FIRST_VALUE(account_type) OVER (PARTITION BY customer_id ORDER BY opened_date DESC, id) first_account_type
FROM accounts
