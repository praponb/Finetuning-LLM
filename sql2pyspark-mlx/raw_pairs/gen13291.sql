WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END AS is_7_eleven,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time >= '2025-08-01'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(is_7_eleven) OVER w_grp AS prev_is_7_eleven,
        LEAD(is_7_eleven) OVER w_grp AS next_is_7_eleven,
        LEAD(amount) OVER w_grp AS next_amount,
        LAG(amount) OVER w_grp AS prev_amount,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM base
    WINDOW w_grp AS (PARTITION BY merchant, txn_type ORDER BY txn_time, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY is_7_eleven, id) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        is_7_eleven,
        merchant_filled,
        prev_amount,
        AVG(prev_amount) OVER (PARTITION BY txn_type, account_id ORDER BY next_amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS prev_amount_avg_5,
        SUM(seq_no) OVER (PARTITION BY account_id ORDER BY prev_is_7_eleven, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no,
        PERCENT_RANK() OVER (PARTITION BY YEAR(txn_time) ORDER BY next_amount DESC) AS next_amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY prev_is_7_eleven DESC, id) AS seq_no_2
    FROM deduped
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        txn_type,
        COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
        SUM(amount) FILTER (WHERE merchant = 'Grab') AS grab_total_amount,
        PERCENTILE_APPROX(prev_amount_avg_5, ARRAY(0.25, 0.5, 0.75)) AS prev_amount_avg_5_quartiles,
        MAX(prev_amount_avg_5) FILTER (WHERE merchant = 'Grab') AS grab_max_prev_amount_avg_5
    FROM with_windows
    GROUP BY txn_type
    HAVING MAX(prev_amount_avg_5) FILTER (WHERE merchant = 'Grab') > 0
)
SELECT
    txn_type,
    debit_count,
    grab_total_amount,
    prev_amount_avg_5_quartiles,
    grab_max_prev_amount_avg_5,
    CASE WHEN txn_type RLIKE '^(REFUND|CREDIT)$' THEN 'CORE' ELSE 'GROUP_B' END AS txn_type_zone
FROM totals;
