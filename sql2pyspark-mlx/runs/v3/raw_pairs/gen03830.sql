SELECT a.status, SUM(CASE WHEN t.txn_type = 'REFUND' THEN 1 ELSE 0 END) refund_count, COUNT(*) num_transactions
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
GROUP BY a.status;
