SELECT t.id, t.account_id, t.merchant
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE MONTH(opened_date) IN (7, 8, 10))
ORDER BY t.id;
