SELECT
    id,
    balance,
    status,
    DAY(opened_date) AS opened_date_day,
    INITCAP(TRIM(account_type)) AS account_type_title
FROM accounts
ORDER BY status, account_type_title;
