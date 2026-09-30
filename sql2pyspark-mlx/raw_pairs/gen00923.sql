WITH metrics AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUM(amount) OVER (PARTITION BY YEAR(txn_time), account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        LAG(amount, 2) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_amount,
        PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount NULLS FIRST) AS amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS seq_no
    FROM transactions
    WHERE amount IS NOT NULL
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.running_amount,
    s.prev_amount,
    s.amount_percent_rank,
    s.seq_no,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM metrics s
ORDER BY id NULLS LAST;
