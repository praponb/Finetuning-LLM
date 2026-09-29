SELECT id, amount FROM transactions WHERE amount >= (SELECT AVG(amount) FROM transactions WHERE is_flagged = TRUE) ORDER BY id ASC LIMIT 1
