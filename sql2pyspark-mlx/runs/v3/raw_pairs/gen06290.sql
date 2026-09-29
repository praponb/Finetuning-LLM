SELECT merchant FROM transactions WHERE is_flagged = TRUE UNION ALL SELECT merchant FROM transactions WHERE TO_DATE(txn_time) >= '2024-04-22' ORDER BY merchant;
