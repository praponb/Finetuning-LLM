SELECT
    id,
    txn_time,
    is_flagged,
    LEAD(amount, 1) OVER (PARTITION BY YEAR(txn_time), txn_type ORDER BY amount, id) AS next_amount,
    MIN(amount) OVER (PARTITION BY account_id) AS account_id_min_amount
FROM transactions;
