WITH filtered AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE account_id IN (7, 8, 18, 28)
),
with_dims AS (
    SELECT
        s.id,
        s.account_id,
        s.amount,
        s.txn_time,
        s.is_flagged,
        s.is_flagged_flag,
        s.resolved_merchant,
        b.account_type,
        b.balance
    FROM filtered s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
metrics AS (
    SELECT
        *,
        COUNT(balance) OVER (PARTITION BY account_type) AS account_type_count_balance,
        LEAD(amount, 1) OVER w_hist AS next_amount
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY YEAR(txn_time), account_id ORDER BY balance, id)
),
with_windows AS (
    SELECT
        *,
        LAG(next_amount) OVER (PARTITION BY account_id, account_type ORDER BY is_flagged_flag, id) AS prev_next_amount,
        LAG(account_type_count_balance) OVER (PARTITION BY account_id, account_type ORDER BY is_flagged_flag, id) AS prev_account_type_count_balance,
        LEAD(account_type_count_balance) OVER (PARTITION BY account_id, account_type ORDER BY is_flagged_flag, id) AS next_account_type_count_balance,
        ROW_NUMBER() OVER (PARTITION BY account_id, account_type ORDER BY is_flagged_flag, id) AS row_seq,
        LAG(balance, 1) OVER (ORDER BY account_type_count_balance, id) AS prev_balance
    FROM metrics
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_windows
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
flagged_2 AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_type ORDER BY txn_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
sessionized_transactions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged_2
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, account_type ORDER BY prev_balance NULLS LAST, id) AS rank_in_group
    FROM sessionized_transactions
    QUALIFY rank_in_group = 1
)
SELECT
    a.account_type AS account_account_type,
    s.account_type AS transaction_account_type,
    COUNT(*) AS rows_total,
    MIN(s.account_type_count_balance) AS min_account_type_count_balance
FROM deduped s
INNER JOIN accounts a ON s.account_id = a.id
GROUP BY ROLLUP (a.account_type, s.account_type)
ORDER BY rows_total DESC NULLS LAST
