SELECT id, amount, is_flagged, txn_time, COALESCE(merchant, 'Unknown') AS merchant_filled FROM transactions ORDER BY amount DESC;
