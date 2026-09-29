WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS FIRST, id) AS rank_in_group
        FROM transactions
    ) t
    WHERE rank_in_group = 1
),
base AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        rank_in_group,
        LEAST(amount, rank_in_group) AS least_amount_rank_in_group
    FROM deduped
    WHERE txn_type = 'CREDIT'
      AND merchant IS NOT DISTINCT FROM 'Starbucks'
),
deduped_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY least_amount_rank_in_group DESC, id) AS rank_in_group_2
    FROM base
    QUALIFY rank_in_group_2 = 1
),
metrics AS (
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        rank_in_group_2,
        DENSE_RANK() OVER (PARTITION BY account_id, merchant ORDER BY amount DESC NULLS FIRST) AS amount_dense_rank,
        AVG(amount) OVER (PARTITION BY YEAR(txn_time) ORDER BY txn_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
        AVG(least_amount_rank_in_group) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS least_amount_rank_in_group_avg_90d,
        LEAD(least_amount_rank_in_group, 1, 0) OVER (PARTITION BY merchant ORDER BY amount, id) AS next_least_amount_rank_in_group,
        AVG(amount) OVER (PARTITION BY YEAR(txn_time), merchant ORDER BY rank_in_group_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3
    FROM deduped_transactions
    WHERE least_amount_rank_in_group > 23
),
flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
sessionized AS (
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        amount_dense_rank,
        amount_avg_5_prior,
        amount_avg_3,
        SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
deduped_transactions_2 AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount_dense_rank DESC NULLS FIRST, id) AS rank_in_group
        FROM sessionized
    ) t
    WHERE rank_in_group = 1
),
filtered AS (
    SELECT *, CASE WHEN amount_avg_5_prior >= 4052 THEN 'High' WHEN amount_avg_5_prior >= 1920 THEN 'Medium' ELSE 'Low' END AS amount_avg_5_prior_level
    FROM deduped_transactions_2
    WHERE txn_time >= TIMESTAMP '2023-10-31 11:00:00'
)
SELECT
    session_no,
    PERCENTILE_APPROX(amount_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS amount_dense_rank_quartiles,
    MIN(txn_time) AS started_at,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY 1
