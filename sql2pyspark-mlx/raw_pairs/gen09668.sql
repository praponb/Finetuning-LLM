SELECT a.id, a.opened_date, a.status
FROM accounts a
WHERE a.balance < 33000 AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount BETWEEN 1300 AND 2500)
