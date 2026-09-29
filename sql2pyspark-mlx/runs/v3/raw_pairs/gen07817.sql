SELECT
    status,
    customer_id,
    opened_date,
    INITCAP(TRIM(status)) AS status_title,
    TRIM(account_type) AS clean_account_type
FROM accounts
