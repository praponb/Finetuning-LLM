SELECT merchant FROM transactions WHERE txn_type = 'REFUND' UNION SELECT merchant FROM transactions WHERE amount BETWEEN 2400 AND 3300 ORDER BY merchant
