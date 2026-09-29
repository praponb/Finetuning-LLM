SELECT is_flagged, COUNT(*) AS row_count
FROM transactions
GROUP BY is_flagged
HAVING COUNT(*) > 4
