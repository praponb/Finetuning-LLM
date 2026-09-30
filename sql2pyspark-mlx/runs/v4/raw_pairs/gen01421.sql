WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY amount DESC NULLS LAST) AS amount_rank,
        SUM(amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM transactions
)
SELECT
    id,
    account_id,
    txn_type,
    merchant,
    cumulative_amount
FROM with_windows
ORDER BY merchant DESC;
