SELECT id, is_flagged, amount, LENGTH(merchant) AS merchant_length FROM transactions WHERE amount > 3300 ORDER BY merchant_length DESC;
