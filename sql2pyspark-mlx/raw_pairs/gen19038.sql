SELECT COUNT(amount) AS amount_count, SUM(amount) AS total_amount FROM transactions WHERE is_flagged OR amount < 1100 ORDER BY 2 DESC;
