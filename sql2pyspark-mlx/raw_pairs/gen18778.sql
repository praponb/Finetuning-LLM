SELECT a.id, a.status, a.opened_date
FROM accounts a
WHERE a.opened_date BETWEEN '2023-11-01' AND '2024-10-01' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id IN (1, 29));
