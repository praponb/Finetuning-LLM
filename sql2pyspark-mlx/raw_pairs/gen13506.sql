SELECT merchant, txn_type FROM transactions WHERE is_flagged UNION ALL SELECT merchant, txn_type FROM transactions WHERE account_id IN (22, 27, 28, 37) ORDER BY merchant;
