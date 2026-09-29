WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(amount AS INT) amount_int,
        TRIM(merchant) clean_merchant
    FROM transactions
    WHERE txn_time >= DATE '2023-11-04'
      AND merchant IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        txn_time,
        is_flagged,
        amount_int,
        clean_merchant,
        DENSE_RANK() OVER w_recent amount_dense_rank,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) amount_avg_2_prior,
        SUM(amount_int) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount_int,
        NTILE(5) OVER (PARTITION BY txn_type ORDER BY amount_int, id) amount_int_bucket
    FROM cleaned
    WINDOW w_recent AS (ORDER BY amount DESC NULLS LAST)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount_dense_rank * 0.85, 2) amount_dense_rank_adjusted,
        ROUND(amount_avg_2_prior * 1.1, 2) amount_avg_2_prior_adjusted
    FROM enriched
    WHERE amount_dense_rank <= 3
),
valid_transactions AS (
    -- Keep recent, valid rows
    SELECT *, amount_dense_rank_adjusted + running_amount_int amount_dense_rank_adjusted_plus_running_amount_int
    FROM valid
    WHERE txn_time > DATE '2023-09-23'
      AND TO_DATE(txn_time) >= '2024-08-29'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_avg_2_prior) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_2_prior_max_90d,
        AVG(amount_int_bucket) OVER (ORDER BY amount_avg_2_prior_adjusted, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) amount_int_bucket_avg_5_prior
    FROM valid_transactions
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount_avg_2_prior_adjusted) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_2_prior_adjusted_avg_90d,
        AVG(amount_dense_rank_adjusted) OVER (ORDER BY running_amount_int, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) amount_dense_rank_adjusted_avg_5_prior,
        SUM(amount_dense_rank_adjusted) OVER (ORDER BY amount_int_bucket_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount_dense_rank_adjusted,
        FIRST_VALUE(amount) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount,
        ROW_NUMBER() OVER (ORDER BY amount_avg_2_prior_adjusted, id) row_seq
    FROM metrics
    WHERE amount_dense_rank_adjusted <= 76
),
base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(amount_dense_rank / amount_avg_2_prior_max_90d, 2) amount_dense_rank_per_amount_avg_2_prior_max_90d
    FROM metrics_transactions
    WHERE txn_time > TIMESTAMP '2023-08-29 02:00:00'
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', txn_time) AS year_start,
        CASE WHEN amount_dense_rank_per_amount_avg_2_prior_max_90d >= 70 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_dense_rank_per_amount_avg_2_prior_max_90d_tier,
        CASE WHEN amount_int >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_int_tier,
        MIN(amount_avg_2_prior_max_90d) AS min_amount_avg_2_prior_max_90d,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(amount_int_bucket_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS amount_int_bucket_avg_5_prior_quartiles
    FROM base
    GROUP BY year_start, amount_dense_rank_per_amount_avg_2_prior_max_90d_tier, amount_int_tier
)
SELECT
    year_start,
    amount_dense_rank_per_amount_avg_2_prior_max_90d_tier,
    amount_int_tier,
    amount_int_bucket_avg_5_prior_quartiles
FROM daily
ORDER BY amount_dense_rank_per_amount_avg_2_prior_max_90d_tier DESC
