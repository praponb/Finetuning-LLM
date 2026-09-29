SELECT
    t.account_id,
    a.opened_date,
    a.customer_id,
    a.balance,
    INITCAP(TRIM(a.account_type)) AS account_type_title
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.merchant = 'Shell' AND t.account_id = 16;
