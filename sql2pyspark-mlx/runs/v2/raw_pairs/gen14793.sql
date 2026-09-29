SELECT t.id, t.is_flagged, t.account_id FROM transactions t WHERE t.account_id IN (SELECT id FROM accounts WHERE balance < 24000) ORDER BY account_id DESC, id LIMIT 5
