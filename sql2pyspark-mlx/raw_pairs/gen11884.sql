WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) AS row_seq,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM transactions
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    row_seq
FROM metrics;
