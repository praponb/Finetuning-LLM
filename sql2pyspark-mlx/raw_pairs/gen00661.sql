SELECT id, txn_time, LENGTH(merchant) AS merchant_length FROM transactions WHERE is_flagged ORDER BY id DESC, merchant_length DESC LIMIT 20;
