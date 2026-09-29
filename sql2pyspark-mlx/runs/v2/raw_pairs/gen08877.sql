SELECT is_flagged, SUM(amount) AS total_amount
FROM transactions
WHERE account_id IN (10, 11, 14, 36) OR amount <= 3600
GROUP BY is_flagged
HAVING total_amount < 23000
