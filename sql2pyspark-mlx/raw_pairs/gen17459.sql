SELECT is_flagged, txn_time, merchant
FROM transactions
WHERE txn_type IN ('CREDIT', 'FEE') AND NOT is_flagged
ORDER BY merchant;
