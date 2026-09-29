WITH scored AS (
    SELECT
        id,
        amount,
        ROW_NUMBER() OVER w_running AS seq_no,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior,
        SUM(amount) OVER (PARTITION BY merchant, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM transactions
    WHERE TO_DATE(txn_time) >= '2025-08-01'
    WINDOW w_running AS (PARTITION BY merchant ORDER BY txn_time DESC, id)
)
SELECT id, amount_avg_2_prior
FROM scored
ORDER BY amount_avg_2_prior NULLS LAST, id DESC
