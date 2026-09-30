SELECT a.id, a.balance
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions WHERE NOT is_flagged)
