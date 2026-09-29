WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        CONCAT(merchant, '/', txn_type) AS merchant_txn_type,
        TRIM(merchant) AS clean_merchant
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Lazada'
      AND txn_time > TIMESTAMP '2023-09-02 00:00:00'
),
joined AS (
    -- Attach accounts attributes
    SELECT src.*, p.balance
    FROM cleaned src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        merchant_txn_type,
        clean_merchant,
        balance,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM joined
    WHERE merchant IS DISTINCT FROM 'Amazon'
      AND txn_time >= '2023-05-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_time,
        is_flagged,
        balance,
        is_flagged_flag,
        LAG(balance) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_balance,
        LEAD(balance) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_balance,
        LEAD(is_flagged_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS next_is_flagged_flag,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS row_seq
    FROM filtered
    WHERE is_flagged
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_flagged,
        balance,
        next_is_flagged_flag,
        LAG(is_flagged_flag) OVER (ORDER BY next_balance, id) AS prev_is_flagged_flag,
        LAG(balance) OVER (ORDER BY next_balance, id) AS prev_balance_2,
        ROW_NUMBER() OVER (ORDER BY next_balance, id) AS row_seq_2,
        DENSE_RANK() OVER (ORDER BY is_flagged_flag DESC NULLS LAST) AS is_flagged_flag_dense_rank
    FROM enriched
),
metrics_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        is_flagged,
        balance,
        next_is_flagged_flag,
        prev_balance_2,
        DENSE_RANK() OVER (ORDER BY balance DESC NULLS FIRST) AS balance_dense_rank,
        AVG(next_is_flagged_flag) OVER (ORDER BY row_seq_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS next_is_flagged_flag_avg_2,
        LEAD(prev_balance_2, 1, 0) OVER w_part AS next_prev_balance_2,
        ROW_NUMBER() OVER (ORDER BY prev_balance_2, id) AS seq_no
    FROM metrics
    WINDOW w_part AS (ORDER BY prev_is_flagged_flag, id)
),
cleaned_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        is_flagged,
        balance,
        next_is_flagged_flag,
        balance_dense_rank,
        next_is_flagged_flag_avg_2,
        CEIL(seq_no) AS seq_no_whole,
        CAST(balance AS INT) AS balance_int
    FROM metrics_transactions
    WHERE seq_no > 7
)
-- Multi-level aggregation
SELECT
    CASE WHEN balance_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS balance_dense_rank_tier,
    CASE WHEN next_is_flagged_flag_avg_2 >= 64 THEN 'HIGH' ELSE 'LOW' END AS next_is_flagged_flag_avg_2_tier,
    PERCENTILE_APPROX(balance_dense_rank, 0.5) AS balance_dense_rank_p50,
    ROUND(AVG(next_is_flagged_flag), 2) AS avg_next_is_flagged_flag,
    MIN(balance_int) AS min_balance_int,
    PERCENTILE_APPROX(seq_no_whole, ARRAY(0.25, 0.5, 0.75)) AS seq_no_whole_quartiles,
    AVG(next_is_flagged_flag) AS avg_next_is_flagged_flag_2,
    MIN(next_is_flagged_flag_avg_2) AS min_next_is_flagged_flag_avg_2
FROM cleaned_transactions
GROUP BY CASE WHEN balance_dense_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_is_flagged_flag_avg_2 >= 64 THEN 'HIGH' ELSE 'LOW' END
HAVING AVG(next_is_flagged_flag) > 0;
