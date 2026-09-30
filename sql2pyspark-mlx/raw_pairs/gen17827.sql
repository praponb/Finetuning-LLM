SELECT id, txn_type, is_flagged, COALESCE(merchant, 'Unknown') AS merchant_filled FROM transactions WHERE amount >= 1800 OR amount >= 3000;
