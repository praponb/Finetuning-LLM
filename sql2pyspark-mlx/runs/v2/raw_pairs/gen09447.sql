SELECT
    id,
    merchant,
    amount,
    DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) amount_dense_rank
FROM transactions
WHERE TO_DATE(txn_time) >= '2025-07-01'
ORDER BY merchant;
