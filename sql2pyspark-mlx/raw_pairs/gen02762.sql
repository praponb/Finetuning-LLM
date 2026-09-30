WITH metrics AS (
    SELECT
        id,
        account_id,
        amount,
        merchant,
        is_flagged,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id, txn_type), 4) AS amount_share,
        COUNT(amount) OVER (PARTITION BY txn_type) AS txn_type_count_amount,
        SUM(amount) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        NTILE(10) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_2
    FROM transactions
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.merchant,
    s.is_flagged,
    s.amount_share,
    s.txn_type_count_amount,
    s.running_amount,
    (SELECT MAX(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS max_transaction_amount
FROM metrics s
ORDER BY account_id DESC NULLS LAST, max_transaction_amount
