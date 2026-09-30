SELECT is_flagged, COUNT(*) AS transaction_count
FROM transactions
GROUP BY 1
ORDER BY transaction_count
