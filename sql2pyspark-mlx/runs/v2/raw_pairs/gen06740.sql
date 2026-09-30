SELECT is_flagged, AVG(amount) avg_amount
FROM transactions
GROUP BY is_flagged
