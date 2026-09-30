SELECT t.id, t.account_id
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE opened_date BETWEEN '2023-04-09' AND '2023-12-01')
