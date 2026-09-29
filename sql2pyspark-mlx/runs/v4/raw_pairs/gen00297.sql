SELECT
    opened_date,
    account_type,
    ABS(balance) AS abs_balance,
    INITCAP(TRIM(status)) AS status_title
FROM accounts
WHERE status IN ('OPEN', 'FROZEN')
