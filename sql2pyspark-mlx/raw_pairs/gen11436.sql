WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        LOWER(txn_type) AS txn_type_lower,
        FLOOR(amount) AS amount_whole,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
      AND NOT is_flagged
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (ORDER BY amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        STDDEV_SAMP(amount_whole) OVER (PARTITION BY account_id) AS account_id_stddev_samp_amount_whole,
        LAST_VALUE(amount_whole) IGNORE NULLS OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_whole,
        ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time DESC, id) AS row_seq
    FROM filtered
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        amount,
        merchant,
        txn_type_lower,
        merchant_filled,
        last_known_amount_whole,
        row_seq,
        AVG(account_id_stddev_samp_amount_whole) OVER w_hist AS running_avg_account_id_stddev_samp_amount_whole,
        SUM(account_id_stddev_samp_amount_whole) OVER w_hist AS running_sum_account_id_stddev_samp_amount_whole,
        MAX(account_id_stddev_samp_amount_whole) OVER w_hist AS running_max_account_id_stddev_samp_amount_whole,
        SUM(account_id_stddev_samp_amount_whole) OVER (ORDER BY amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_account_id_stddev_samp_amount_whole
    FROM enriched
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        merchant_filled,
        running_sum_account_id_stddev_samp_amount_whole,
        running_max_account_id_stddev_samp_amount_whole,
        STDDEV_SAMP(running_sum_account_id_stddev_samp_amount_whole) OVER (PARTITION BY merchant) AS merchant_stddev_samp_running_sum_account_id_stddev_samp_amount_whole,
        LEAD(running_max_account_id_stddev_samp_amount_whole, 1) OVER (PARTITION BY account_id ORDER BY running_sum_account_id_stddev_samp_amount_whole, id) AS next_running_max_account_id_stddev_samp_amount_whole,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY cumulative_account_id_stddev_samp_amount_whole DESC, id) AS cumulative_account_id_stddev_samp_amount_whole_bucket,
        PERCENT_RANK() OVER w_grp AS amount_percent_rank,
        COUNT(running_sum_account_id_stddev_samp_amount_whole) OVER (PARTITION BY account_id) AS account_id_count_running_sum_account_id_stddev_samp_amount_whole
    FROM scored
    WINDOW w_grp AS (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST)
),
scored_transactions AS (
    -- Running and moving aggregates
    SELECT
        id,
        merchant_stddev_samp_running_sum_account_id_stddev_samp_amount_whole,
        amount_percent_rank,
        account_id_count_running_sum_account_id_stddev_samp_amount_whole,
        FIRST_VALUE(running_sum_account_id_stddev_samp_amount_whole, TRUE) OVER w_part AS first_known_running_sum_account_id_stddev_samp_amount_whole,
        FIRST_VALUE(running_max_account_id_stddev_samp_amount_whole) IGNORE NULLS OVER w_part AS first_known_running_max_account_id_stddev_samp_amount_whole
    FROM with_windows
    WINDOW w_part AS (ORDER BY merchant_stddev_samp_running_sum_account_id_stddev_samp_amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(amount_percent_rank, first_known_running_sum_account_id_stddev_samp_amount_whole) AS greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole,
        first_known_running_max_account_id_stddev_samp_amount_whole + amount_percent_rank AS first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank
    FROM scored_transactions
    WHERE first_known_running_sum_account_id_stddev_samp_amount_whole <= 120
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id_count_running_sum_account_id_stddev_samp_amount_whole,
        first_known_running_sum_account_id_stddev_samp_amount_whole,
        first_known_running_max_account_id_stddev_samp_amount_whole,
        greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole,
        first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank,
        CAST(greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole AS INT) AS greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int
    FROM base
    WHERE greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole BETWEEN 27 AND 30
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole >= 79 THEN 'HIGH' ELSE 'LOW' END AS greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_tier,
        COUNT(*) AS event_count,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank), 2) AS avg_first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank,
        PERCENTILE_APPROX(account_id_count_running_sum_account_id_stddev_samp_amount_whole, ARRAY(0.25, 0.5, 0.75)) AS account_id_count_running_sum_account_id_stddev_samp_amount_whole_quartiles,
        SUM(greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int) AS total_greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int
    FROM cleaned
    GROUP BY 1
    HAVING COUNT(*) > 1
)
SELECT
    greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_tier,
    distinct_ids,
    avg_first_known_running_max_account_id_stddev_samp_amount_whole_plus_amount_percent_rank,
    account_id_count_running_sum_account_id_stddev_samp_amount_whole_quartiles,
    total_greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_int
FROM totals
ORDER BY distinct_ids DESC, greatest_amount_percent_rank_first_known_running_sum_account_id_stddev_samp_amount_whole_tier
