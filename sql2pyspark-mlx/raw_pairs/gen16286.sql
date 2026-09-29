SELECT t.id, t.merchant, t.amount, t.is_flagged FROM transactions t WHERE t.amount < 3900 AND t.account_id IN (SELECT id FROM accounts WHERE balance <= 17000) ORDER BY amount DESC, id LIMIT 1
