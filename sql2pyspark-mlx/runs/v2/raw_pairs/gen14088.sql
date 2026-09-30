SELECT
    id,
    merchant,
    txn_type,
    DATE_TRUNC('YEAR', txn_time) AS txn_time_period
FROM transactions
WHERE is_flagged
ORDER BY merchant;
