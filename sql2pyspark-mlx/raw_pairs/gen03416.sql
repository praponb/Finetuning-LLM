WITH latest AS (
    SELECT *
    FROM transactions
    QUALIFY RANK() OVER (PARTITION BY merchant ORDER BY amount ASC NULLS LAST) = 1
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM latest
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
metrics AS (
    SELECT
        *,
        LEAD(amount, 1) OVER w_ordered next_amount,
        SUM(island_id) OVER (PARTITION BY merchant) merchant_sum_island_id,
        AVG(island_id) OVER (PARTITION BY TO_DATE(txn_time) ORDER BY island_id ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) island_id_avg_2,
        PERCENT_RANK() OVER w_part new_group_flag_percent_rank,
        LEAD(island_id, 1) OVER (PARTITION BY account_id ORDER BY txn_time ASC, id ASC) next_island_id
    FROM sessionized
    WINDOW w_ordered AS (PARTITION BY TO_DATE(txn_time), txn_type ORDER BY txn_time ASC, id ASC), w_part AS (PARTITION BY txn_type ORDER BY new_group_flag ASC NULLS FIRST)
),
filtered AS (
    SELECT
        *,
        ROUND(island_id, 1) island_id_rounded,
        COALESCE(merchant, 'N/A') merchant_clean
    FROM metrics
    WHERE txn_time >= TO_DATE('2023-10-20')
      AND merchant IS NOT NULL
)
SELECT
    *,
    COUNT(new_group_flag) OVER (PARTITION BY merchant) merchant_count_new_group_flag,
    DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY island_id DESC) island_id_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY txn_time DESC, id ASC) row_seq
FROM filtered;
