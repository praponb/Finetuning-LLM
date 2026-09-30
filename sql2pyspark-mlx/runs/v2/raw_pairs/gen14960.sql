SELECT id, account_id FROM transactions WHERE NOT (txn_type IN ('DEBIT', 'CREDIT', 'REFUND') AND merchant IS NULL AND account_id IN (1, 12, 25, 33));
