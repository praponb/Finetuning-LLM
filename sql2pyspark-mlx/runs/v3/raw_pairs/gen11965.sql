SELECT id, is_flagged, amount, ROUND(amount * 1.07, 2) AS amount_scaled FROM transactions WHERE account_id = 11 AND account_id = 3
