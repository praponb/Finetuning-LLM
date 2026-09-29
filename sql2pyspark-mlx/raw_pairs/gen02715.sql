SELECT CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'tier_1' ELSE 'other' END status_group, MAX(balance) - MIN(balance) balance_range
FROM accounts
WHERE balance BETWEEN 20000 AND 76000 OR customer_id IS NOT NULL
GROUP BY CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'tier_1' ELSE 'other' END;
