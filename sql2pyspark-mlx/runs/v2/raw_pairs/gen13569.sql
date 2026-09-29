SELECT id, account_id FROM transactions WHERE merchant IS NULL INTERSECT SELECT id, account_id FROM transactions WHERE txn_type NOT IN ('REFUND', 'FEE', 'DEBIT');
