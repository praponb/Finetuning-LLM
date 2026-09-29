SELECT
    id,
    customer_id,
    opened_date,
    INITCAP(TRIM(account_type)) AS account_type_title
FROM accounts
WHERE status NOT IN ('CLOSED', 'OPEN') AND balance > 52000 AND status = 'CLOSED'
ORDER BY id DESC
