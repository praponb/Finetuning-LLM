SELECT CASE WHEN a.status IN ('CLOSED', 'FROZEN') THEN 'tier_1' ELSE 'other' END AS status_group, CASE WHEN a.balance >= 79000 THEN 'high' WHEN a.balance >= 7000 THEN 'mid' ELSE 'low' END AS balance_bucket, SORT_ARRAY(COLLECT_SET(t.txn_type)) AS txn_type_list
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
GROUP BY CASE WHEN a.status IN ('CLOSED', 'FROZEN') THEN 'tier_1' ELSE 'other' END, CASE WHEN a.balance >= 79000 THEN 'high' WHEN a.balance >= 7000 THEN 'mid' ELSE 'low' END
ORDER BY 1 DESC;
