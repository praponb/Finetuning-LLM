WITH filtered AS (
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2023-08-25'
      AND merchant IS NOT DISTINCT FROM 'Lazada'
),
cleaned AS (
    SELECT
        *,
        ROUND(amount_int / amount, 2) AS amount_int_per_amount,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM filtered
    WHERE txn_time > DATE '2023-11-01'
),
metrics AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant_clean,
        merchant_filled,
        LAST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        ROW_NUMBER() OVER (PARTITION BY MONTH(txn_time) ORDER BY amount, id) AS row_seq
    FROM cleaned
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessions_transactions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_dims AS (
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.txn_time,
        s.row_seq,
        s.is_new_burst,
        a.opened_date
    FROM sessions_transactions s
    LEFT JOIN accounts a
        ON s.account_id = a.id
    WHERE MONTH(a.opened_date) IN (6, 10, 12)
)
SELECT
    CASE WHEN is_new_burst >= 1 THEN 'LARGE' ELSE 'SMALL' END AS is_new_burst_tier,
    CASE WHEN row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    APPROX_PERCENTILE(is_new_burst, ARRAY(0.25, 0.5, 0.75)) AS is_new_burst_quartiles,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types
FROM with_dims
GROUP BY 1, 2, 3;
