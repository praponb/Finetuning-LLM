SELECT
    a.id,
    a.status,
    a.customer_id,
    a.opened_date
FROM accounts a
WHERE a.account_type IN ('SAVINGS', 'CREDIT') AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id = 13)
ORDER BY customer_id DESC
