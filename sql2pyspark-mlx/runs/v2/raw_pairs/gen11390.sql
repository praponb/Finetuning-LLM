SELECT
    id,
    balance,
    opened_date,
    status,
    INITCAP(TRIM(account_type)) AS account_type_title
FROM accounts
ORDER BY balance, id
LIMIT 5;
