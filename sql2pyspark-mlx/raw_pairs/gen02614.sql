SELECT is_flagged, COUNT(DISTINCT amount) AS unique_txn_types
FROM transactions
WHERE merchant != '7-Eleven' AND amount <= 2800
GROUP BY is_flagged
ORDER BY unique_txn_types DESC;
