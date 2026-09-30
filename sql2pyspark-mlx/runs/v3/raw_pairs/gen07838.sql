SELECT merchant, MIN(amount) AS min_amount, COUNT(amount) AS amount_count FROM transactions WHERE (amount > 3100 AND amount IS NULL) OR is_flagged = TRUE GROUP BY merchant
