SELECT id, amount, is_flagged, account_id FROM transactions WHERE amount > (SELECT AVG(amount) FROM transactions);
