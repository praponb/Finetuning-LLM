SELECT id, amount, is_flagged, merchant FROM transactions WHERE amount > (SELECT MAX(amount) FROM transactions) ORDER BY id DESC
