SELECT t.merchant, t.account_id, a.customer_id FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE t.txn_type IN ('FEE', 'DEBIT', 'REFUND') ORDER BY account_id;
