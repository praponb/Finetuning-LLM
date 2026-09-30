SELECT id, amount, is_flagged, merchant FROM transactions WHERE amount < (SELECT MIN(amount) FROM transactions) ORDER BY id DESC;
