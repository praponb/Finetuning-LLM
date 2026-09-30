WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(amount AS INT) amount_int,
        COALESCE(merchant, 'OTHER') merchant_filled
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Shell'
      AND account_id IN (11, 36)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_time,
        merchant,
        amount_int,
        merchant_filled,
        PERCENT_RANK() OVER (PARTITION BY merchant, txn_type ORDER BY amount_int DESC NULLS FIRST) amount_int_percent_rank,
        SUM(amount_int) OVER (PARTITION BY merchant, txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount_int,
        RANK() OVER (PARTITION BY TO_DATE(txn_time), txn_type ORDER BY amount_int) amount_int_rank
    FROM base
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_time,
        amount_int,
        merchant_filled,
        amount_int_percent_rank,
        cumulative_amount_int,
        amount_int_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount_int DESC, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
base_transactions AS (
    -- Basic cleansing and defaults
    SELECT *, cumulative_amount_int - amount_int cumulative_amount_int_minus_amount_int
    FROM deduped
    WHERE txn_time > DATE '2023-10-17'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_time,
        amount_int,
        merchant_filled,
        amount_int_percent_rank,
        amount_int_rank,
        cumulative_amount_int_minus_amount_int,
        DATE_FORMAT(txn_time, 'yyyy-MM') txn_time_label
    FROM base_transactions
    WHERE txn_time >= TIMESTAMP '2023-10-17 19:00:00'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount_int_rank) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_int_rank_sum_90d,
        LEAD(amount_int_rank, 1) OVER (ORDER BY amount_int, id) next_amount_int_rank
    FROM cleaned
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        merchant_filled,
        amount_int_percent_rank,
        amount_int_rank_sum_90d,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('WEEK', txn_time) ORDER BY amount_int_rank_sum_90d, id) amount_int_rank_sum_90d_bucket,
        LEAD(amount_int_percent_rank, 1) OVER w_part next_amount_int_percent_rank
    FROM with_windows
    WINDOW w_part AS (ORDER BY txn_time, id)
)
SELECT
    id,
    merchant_filled,
    amount_int_percent_rank,
    amount_int_rank_sum_90d,
    amount_int_rank_sum_90d_bucket
FROM with_windows_transactions
ORDER BY amount_int_rank_sum_90d_bucket DESC NULLS LAST;
