WITH flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
base AS (
    SELECT
        *,
        ROUND(amount, 0) AS amount_rounded,
        island_id + amount AS island_id_plus_amount,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM islands
    WHERE txn_time >= TO_DATE('2023-04-12')
      AND merchant IS NOT NULL
),
cleaned AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        island_id,
        amount_rounded,
        island_id_plus_amount,
        resolved_merchant,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END AS is_credit
    FROM base
    WHERE txn_time >= TO_TIMESTAMP('2023-06-12 05:00:00')
),
cleaned_transactions AS (
    SELECT *, COALESCE(island_id_plus_amount, 0) AS island_id_plus_amount_filled
    FROM cleaned
    WHERE txn_time > TIMESTAMP '2023-07-02 20:00:00'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY island_id_plus_amount NULLS LAST, id) AS row_num
        FROM cleaned_transactions
    ) ranked
    WHERE row_num = 1
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM latest
),
sessionized AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        is_flagged_flag,
        island_id_plus_amount_filled,
        row_num,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM with_gaps
),
metrics AS (
    SELECT
        id,
        txn_time,
        is_flagged,
        is_flagged_flag,
        island_id_plus_amount_filled,
        row_num,
        SUM(island_id_plus_amount_filled) OVER w_hist AS last4_sum_island_id_plus_amount_filled,
        COUNT(island_id_plus_amount_filled) OVER w_hist AS last4_count_island_id_plus_amount_filled,
        AVG(island_id_plus_amount_filled) OVER w_hist AS last4_avg_island_id_plus_amount_filled,
        MIN(island_id_plus_amount_filled) OVER w_hist AS last4_min_island_id_plus_amount_filled
    FROM sessionized
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY island_id_plus_amount_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN last4_min_island_id_plus_amount_filled >= 63 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_island_id_plus_amount_filled_tier,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    MAX(txn_time) AS last_txn_time,
    PERCENTILE_APPROX(last4_min_island_id_plus_amount_filled, ARRAY(0.25, 0.5, 0.75)) AS last4_min_island_id_plus_amount_filled_quartiles,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(is_flagged_flag, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_flag_quartiles,
    ROUND(AVG(last4_count_island_id_plus_amount_filled), 2) AS avg_last4_count_island_id_plus_amount_filled,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY CASE WHEN last4_min_island_id_plus_amount_filled >= 63 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', txn_time);
