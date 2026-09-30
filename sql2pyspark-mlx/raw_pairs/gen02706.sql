WITH scored AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY monthly_fee, id) monthly_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY MONTH(start_date), status ORDER BY start_date DESC, id) row_seq,
        NTILE(3) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) monthly_fee_bucket_2,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) monthly_fee_avg_3
    FROM subscriptions
    WHERE status = 'PAUSED'
),
base AS (
    SELECT
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_bucket,
        row_seq,
        monthly_fee_avg_3,
        monthly_fee_avg_3 - row_seq monthly_fee_avg_3_minus_row_seq,
        ROUND(monthly_fee_bucket_2 / row_seq, 2) monthly_fee_bucket_2_per_row_seq
    FROM scored
    WHERE YEAR(start_date) >= 2023
      AND customer_id IS NOT NULL
),
base_subscriptions AS (
    SELECT
        id,
        start_date,
        end_date,
        status,
        monthly_fee_bucket,
        row_seq,
        monthly_fee_avg_3,
        monthly_fee_avg_3_minus_row_seq,
        monthly_fee_bucket_2_per_row_seq,
        FLOOR(monthly_fee_avg_3) monthly_fee_avg_3_whole,
        ROUND(DATEDIFF(end_date, start_date) / 30, 1) months_start_date_to_end_date
    FROM base
    WHERE customer_id IS NOT NULL
      AND monthly_fee_avg_3_minus_row_seq IS NOT NULL
),
filtered AS (
    SELECT
        id,
        start_date,
        end_date,
        status,
        monthly_fee_bucket,
        row_seq,
        monthly_fee_avg_3,
        monthly_fee_avg_3_minus_row_seq,
        monthly_fee_bucket_2_per_row_seq,
        months_start_date_to_end_date,
        monthly_fee_avg_3_whole * monthly_fee_avg_3_minus_row_seq monthly_fee_avg_3_whole_times_monthly_fee_avg_3_minus_row_seq,
        months_start_date_to_end_date * monthly_fee_bucket_2_per_row_seq months_start_date_to_end_date_times_monthly_fee_bucket_2_per_row_seq
    FROM base_subscriptions
    WHERE monthly_fee_avg_3_whole >= 29
),
with_windows AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY status ORDER BY row_seq, id) row_seq_bucket,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY status), 4) row_seq_share
    FROM filtered
),
latest AS (
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY end_date) = 1
),
cleaned AS (
    SELECT
        id,
        end_date,
        status,
        monthly_fee_bucket,
        row_seq,
        monthly_fee_avg_3,
        monthly_fee_avg_3_minus_row_seq,
        monthly_fee_bucket_2_per_row_seq,
        months_start_date_to_end_date,
        monthly_fee_avg_3_whole_times_monthly_fee_avg_3_minus_row_seq,
        ROUND(monthly_fee_bucket_2_per_row_seq * 0.85, 2) monthly_fee_bucket_2_per_row_seq_adjusted
    FROM latest
    WHERE end_date > TO_DATE('2023-04-17')
)
SELECT
    status,
    DATE_TRUNC('QUARTER', end_date) AS quarter_start,
    DATE_TRUNC('WEEK', end_date) AS week_start,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee_bucket_2_per_row_seq_adjusted ELSE 0 END) AS active_monthly_fee_bucket_2_per_row_seq_adjusted,
    SUM(months_start_date_to_end_date) AS total_months_start_date_to_end_date,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE status = 'ACTIVE') AS active_rows
FROM cleaned
GROUP BY CUBE (status, DATE_TRUNC('QUARTER', end_date), DATE_TRUNC('WEEK', end_date))
ORDER BY active_rows NULLS FIRST
