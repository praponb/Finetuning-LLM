WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE merchant IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_label,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY amount, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d
    FROM cleaned
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged_label,
        amount_sum_7d,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label_2
    FROM scored
    WHERE txn_time >= DATE '2023-09-21'
      AND account_id = 24
),
scored_transactions AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged_label,
        amount_sum_7d,
        is_flagged_flag,
        is_flagged_label_2,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY amount_sum_7d, id) AS amount_sum_7d_bucket,
        AVG(is_flagged_flag) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS is_flagged_flag_avg_5
    FROM filtered
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        txn_type,
        DATE_TRUNC('YEAR', txn_time) AS year_start,
        COUNT(*) FILTER (WHERE txn_type = 'FEE') AS fee_rows,
        COUNT(CASE WHEN txn_type = 'CREDIT' THEN 1 END) AS credit_count,
        APPROX_PERCENTILE(amount_sum_7d_bucket, 0.5) AS amount_sum_7d_bucket_p50,
        GROUPING(txn_type) AS grouping_id
    FROM scored_transactions
    GROUP BY GROUPING SETS ((txn_type, DATE_TRUNC('YEAR', txn_time)), (txn_type), ())
)
SELECT
    txn_type,
    year_start,
    fee_rows,
    credit_count,
    amount_sum_7d_bucket_p50,
    grouping_id,
    CASE WHEN txn_type RLIKE '^(REFUND|DEBIT)$' THEN 'OTHER_ZONE' WHEN txn_type RLIKE '^(CREDIT|FEE)$' THEN 'EXTENDED' ELSE 'CORE' END AS txn_type_zone
FROM summary;
