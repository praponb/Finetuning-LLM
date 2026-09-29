SELECT
    a.id,
    a.status,
    a.balance,
    a.customer_id
FROM accounts a
WHERE a.opened_date BETWEEN '2023-10-21' AND '2025-03-01' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (16, 28))
