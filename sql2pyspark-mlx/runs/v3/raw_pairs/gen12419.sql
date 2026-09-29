SELECT COUNT(amount) AS amount_count, COUNT(*) AS num_transactions FROM transactions WHERE is_flagged ORDER BY num_transactions DESC
