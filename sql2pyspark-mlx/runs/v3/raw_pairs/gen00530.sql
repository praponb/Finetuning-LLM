SELECT account_id, COUNT(CASE WHEN merchant = 'Starbucks' THEN 1 END) AS starbucks_count, ROUND((100 * SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM transactions
GROUP BY account_id
HAVING pct_credit < 49
ORDER BY pct_credit;
