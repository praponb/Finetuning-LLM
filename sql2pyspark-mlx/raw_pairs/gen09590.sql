WITH with_windows AS (
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        LAST_VALUE(merchant, TRUE) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_merchant,
        SUM(amount) OVER w_part cumulative_amount,
        SUM(amount) OVER w_hist running_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY merchant, txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount
    FROM transactions
    WINDOW w_part AS (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY MONTH(txn_time), account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    is_flagged,
    running_amount,
    first_known_amount,
    CASE WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'GROUP_B' WHEN txn_type RLIKE '^(CREDIT|FEE)$' THEN 'PRIORITY' ELSE 'CORE' END AS txn_type_zone
FROM with_windows
ORDER BY running_amount DESC NULLS LAST
