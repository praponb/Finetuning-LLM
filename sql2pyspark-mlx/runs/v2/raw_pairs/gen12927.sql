SELECT merchant, COUNT(*) AS row_count
FROM transactions
GROUP BY merchant
ORDER BY row_count DESC, merchant
LIMIT 5;
