SELECT id, merchant FROM transactions WHERE txn_type IN ('REFUND', 'DEBIT', 'FEE') UNION SELECT id, merchant FROM transactions WHERE merchant NOT IN ('Lazada', 'Amazon', 'Grab') ORDER BY id DESC;
