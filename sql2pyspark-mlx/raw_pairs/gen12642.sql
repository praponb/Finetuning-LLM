SELECT t.id, t.is_flagged, t.account_id, t.amount FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE customer_id IS NULL)
