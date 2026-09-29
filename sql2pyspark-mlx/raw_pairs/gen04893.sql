SELECT
    a.id,
    a.opened_date,
    a.account_type,
    a.status
FROM accounts a
WHERE a.opened_date > '2025-01-01' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
