WITH with_dims AS (
    SELECT a.*, b.balance
    FROM transactions a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
metrics AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id, txn_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        NTILE(5) OVER (ORDER BY balance DESC, id) AS balance_bucket,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY YEAR(txn_time), txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM with_dims
    WHERE HOUR(txn_time) BETWEEN 8 AND 11
),
with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
enriched AS (
    SELECT
        *,
        LEAD(balance, 2, 0) OVER (ORDER BY balance_bucket, id) AS next_balance,
        PERCENT_RANK() OVER (PARTITION BY TO_DATE(txn_time), merchant ORDER BY amount NULLS LAST) AS amount_percent_rank
    FROM islands
),
filtered AS (
    SELECT
        *,
        session_no + last_known_amount AS session_no_plus_last_known_amount,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM enriched
    WHERE txn_time >= TO_TIMESTAMP('2023-06-04 06:00:00')
      AND amount_sum_7d < 14299
)
SELECT *
FROM (
    SELECT
        txn_type,
        merchant,
        amount_sum_7d
    FROM filtered
) src
PIVOT (
    SUM(amount_sum_7d)
    FOR merchant IN ('Shell' AS shell, 'Lazada' AS lazada)
);
