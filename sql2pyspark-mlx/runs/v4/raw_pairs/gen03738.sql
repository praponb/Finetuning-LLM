SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS seq_no,
    SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
FROM transactions
