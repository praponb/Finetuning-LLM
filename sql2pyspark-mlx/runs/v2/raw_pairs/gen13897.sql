SELECT a.status, SUM(CASE WHEN t.txn_type = 'CREDIT' THEN 1 ELSE 0 END) credit_count, MIN(t.amount) min_amount FROM transactions t INNER JOIN accounts a ON t.account_id = a.id GROUP BY a.status
