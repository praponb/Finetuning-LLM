SELECT
    id,
    status,
    account_type,
    opened_date,
    DATE_SUB(opened_date, 78) AS opened_date_minus_78d
FROM accounts
WHERE account_type IN ('CREDIT', 'SAVINGS', 'CHECKING') AND MONTH(opened_date) IN (3, 4, 12)
ORDER BY id DESC
