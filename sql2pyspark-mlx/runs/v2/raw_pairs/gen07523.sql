SELECT merchant, account_id FROM transactions WHERE txn_type IN ('FEE', 'REFUND', 'CREDIT') UNION ALL SELECT merchant, account_id FROM transactions WHERE amount > 1800 ORDER BY account_id DESC
