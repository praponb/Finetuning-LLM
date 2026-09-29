SELECT id, amount, is_flagged, COALESCE(merchant, 'unassigned') AS merchant_filled, ROUND(amount, 0) AS amount_rounded FROM transactions WHERE merchant = 'Shell';
