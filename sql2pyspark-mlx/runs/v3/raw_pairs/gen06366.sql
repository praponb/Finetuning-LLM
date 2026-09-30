SELECT CASE WHEN txn_type IN ('FEE', 'DEBIT') THEN 'tier_1' ELSE 'other' END AS txn_type_group, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count
FROM transactions
WHERE txn_type = 'CREDIT'
GROUP BY txn_type_group
HAVING shell_count >= 34
ORDER BY txn_type_group DESC;
