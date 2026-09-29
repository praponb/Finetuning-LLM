SELECT is_flagged, ROUND(AVG(amount), 2) AS avg_amount, COUNT(*) AS row_count
FROM transactions
GROUP BY is_flagged
