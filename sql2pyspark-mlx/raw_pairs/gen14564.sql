SELECT id, is_flagged, amount FROM transactions WHERE amount IS NOT NULL AND (merchant IN ('7-Eleven', 'Amazon') OR txn_type = 'FEE') ORDER BY amount, id LIMIT 10
