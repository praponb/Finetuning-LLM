SELECT a.status, COUNT(t.id) AS transaction_count FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.txn_type IN ('CREDIT', 'FEE', 'REFUND') GROUP BY a.status ORDER BY 2 DESC
