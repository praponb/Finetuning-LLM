SELECT txn_type, ROUND((100 * SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_lazada
FROM transactions
WHERE account_id IN (16, 35)
GROUP BY txn_type
ORDER BY pct_lazada DESC;
