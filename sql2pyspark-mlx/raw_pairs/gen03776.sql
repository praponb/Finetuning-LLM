SELECT is_flagged, txn_type, merchant, COUNT(amount) AS amount_count, MAX(txn_time) AS last_txn_time FROM transactions WHERE amount BETWEEN 3600 AND 3800 GROUP BY is_flagged, txn_type, merchant;
