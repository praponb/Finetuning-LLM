SELECT is_flagged, COUNT(amount) AS amount_count, SUM(amount) AS total_amount
FROM transactions
WHERE NOT (is_flagged = FALSE AND txn_time >= '2024-12-01' AND TO_DATE(txn_time) >= '2025-01-01')
GROUP BY is_flagged
ORDER BY total_amount, is_flagged
LIMIT 10
