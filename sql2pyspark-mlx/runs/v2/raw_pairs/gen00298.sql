SELECT amount, txn_type, account_id, COALESCE(merchant, 'Unknown') AS merchant_filled FROM transactions;
