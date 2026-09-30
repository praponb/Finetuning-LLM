SELECT account_id, id FROM transactions EXCEPT ALL SELECT account_id, id FROM transactions WHERE txn_type = 'DEBIT';
