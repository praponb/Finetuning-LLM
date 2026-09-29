SELECT
    id,
    opened_date,
    account_type,
    customer_id,
    DATE_SUB(opened_date, 40) opened_date_minus_40d,
    SUBSTRING(status, 1, 2) status_prefix
FROM accounts
