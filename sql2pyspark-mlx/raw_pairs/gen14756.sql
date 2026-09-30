WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        paid_at,
        ROW_NUMBER() OVER w_hist AS seq_no,
        AVG(amount) OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior
    FROM payments
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('MONTH', paid_at), method ORDER BY paid_at DESC, id ASC)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, TO_DATE(paid_at) AS paid_at_date
    FROM metrics
    WHERE paid_at > TIMESTAMP '2023-11-14 16:00:00'
      AND HOUR(paid_at) BETWEEN 12 AND 17
),
valid_payments AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        paid_at,
        amount_avg_3_prior,
        paid_at_date,
        ROUND(amount_avg_3_prior * 0.9, 2) AS amount_avg_3_prior_scaled,
        seq_no - amount_avg_3_prior AS seq_no_minus_amount_avg_3_prior
    FROM valid
    WHERE TO_DATE(paid_at) < '2024-02-01'
),
valid_payments_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(paid_at_date, 3) AS paid_at_date_plus_months,
        COALESCE(amount_avg_3_prior_scaled, 0) AS amount_avg_3_prior_scaled_filled
    FROM valid_payments
    WHERE paid_at > DATE '2023-10-16'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        paid_at_date,
        amount_avg_3_prior_scaled,
        paid_at_date_plus_months,
        COALESCE(amount_avg_3_prior, 0) AS amount_avg_3_prior_filled,
        COALESCE(amount_avg_3_prior_scaled_filled, 0) AS amount_avg_3_prior_scaled_filled_filled
    FROM valid_payments_2
    WHERE paid_at > TIMESTAMP '2023-10-29 22:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount_avg_3_prior_scaled,
        amount_avg_3_prior_scaled_filled_filled,
        RANK() OVER (ORDER BY amount_avg_3_prior_scaled_filled_filled DESC NULLS LAST) AS amount_avg_3_prior_scaled_filled_filled_rank,
        LEAD(amount_avg_3_prior_scaled, 1) OVER (ORDER BY amount_avg_3_prior_scaled ASC, id ASC) AS next_amount_avg_3_prior_scaled,
        NTILE(10) OVER (ORDER BY amount_avg_3_prior_filled DESC, id ASC) AS amount_avg_3_prior_filled_bucket
    FROM cleaned
    WHERE amount_avg_3_prior_filled >= 34
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(amount_avg_3_prior_filled_bucket) OVER w_grp AS running_min_amount_avg_3_prior_filled_bucket,
        SUM(amount_avg_3_prior_filled_bucket) OVER w_grp AS running_sum_amount_avg_3_prior_filled_bucket,
        COUNT(amount_avg_3_prior_filled_bucket) OVER w_grp AS running_count_amount_avg_3_prior_filled_bucket,
        AVG(amount_avg_3_prior_scaled_filled_filled_rank) OVER (ORDER BY amount_avg_3_prior_scaled_filled_filled_rank ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior_scaled_filled_filled_rank_avg_2_prior
    FROM scored
    WINDOW w_grp AS (ORDER BY amount_avg_3_prior_scaled_filled_filled_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    amount_avg_3_prior_scaled,
    amount_avg_3_prior_filled_bucket,
    running_min_amount_avg_3_prior_filled_bucket,
    running_sum_amount_avg_3_prior_filled_bucket,
    running_count_amount_avg_3_prior_filled_bucket
FROM with_windows
ORDER BY amount_avg_3_prior_scaled DESC NULLS FIRST;
