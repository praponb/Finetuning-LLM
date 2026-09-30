SELECT t.id, t.account_id
FROM transactions t
WHERE NOT t.is_flagged AND t.account_id IN (SELECT id FROM accounts WHERE opened_date > '2025-06-01');
