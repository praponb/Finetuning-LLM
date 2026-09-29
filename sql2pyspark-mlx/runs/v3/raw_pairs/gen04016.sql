SELECT CASE WHEN a.status IN ('OPEN', 'CLOSED') THEN 'core' ELSE 'other' END AS status_group, SUM(CASE WHEN t.merchant = 'Lazada' THEN t.amount ELSE 0 END) AS lazada_amount, ROUND((100 * SUM(CASE WHEN t.txn_type = 'REFUND' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refund
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.txn_type != 'CREDIT'
GROUP BY CASE WHEN a.status IN ('OPEN', 'CLOSED') THEN 'core' ELSE 'other' END;
