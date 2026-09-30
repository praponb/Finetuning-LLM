SELECT
    a.id,
    a.customer_id,
    a.status,
    a.opened_date
FROM accounts a
WHERE a.status NOT IN ('OPEN', 'FROZEN') AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
