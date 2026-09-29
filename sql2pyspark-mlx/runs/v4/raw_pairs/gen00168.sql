SELECT *, CASE WHEN amount >= 3400 THEN 'High' WHEN amount >= 1200 THEN 'Medium' ELSE 'Low' END AS amount_tier, INITCAP(TRIM(merchant)) AS merchant_title
FROM transactions
WHERE txn_type = 'DEBIT'
ORDER BY txn_time, id;
