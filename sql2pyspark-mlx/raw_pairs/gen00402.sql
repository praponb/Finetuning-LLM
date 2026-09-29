SELECT is_flagged, merchant, ROUND(AVG(amount), 2) avg_amount
FROM transactions
GROUP BY is_flagged, merchant
ORDER BY merchant, avg_amount DESC, is_flagged
LIMIT 50
