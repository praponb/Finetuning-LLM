WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        account_id,
        txn_type,
        txn_time
    FROM transactions
    QUALIFY RANK() OVER (PARTITION BY txn_type, account_id ORDER BY txn_time DESC) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(txn_time), txn_type ORDER BY txn_time, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS row_seq_2,
        ROW_NUMBER() OVER (ORDER BY txn_time, id) AS event_seq_2
    FROM latest
    WHERE account_id IN (9, 30, 37, 38)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, account_id
    FROM enriched
    EXCEPT ALL
    SELECT id, account_id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 8 AND 13
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id AND s.account_id = k.account_id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        event_seq,
        row_seq,
        row_seq_2,
        event_seq_2,
        FLOOR(row_seq) AS row_seq_whole,
        CASE WHEN row_seq_2 >= 8 THEN 'High' WHEN row_seq_2 >= 2 THEN 'Medium' ELSE 'Low' END AS row_seq_2_band
    FROM verified
    WHERE event_seq_2 BETWEEN 5 AND 7
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.event_seq,
        src.row_seq_2,
        src.event_seq_2,
        src.row_seq_whole,
        src.row_seq_2_band,
        p.opened_date,
        p.balance,
        p.account_type
    FROM base src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        event_seq,
        row_seq_2,
        row_seq_2_band,
        opened_date,
        balance,
        MAX(row_seq_whole) OVER (PARTITION BY account_type) AS account_type_max_row_seq_whole,
        COUNT(balance) OVER (PARTITION BY account_type) AS account_type_count_balance
    FROM with_dims
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        balance,
        account_type_max_row_seq_whole,
        account_type_count_balance,
        PERCENT_RANK() OVER w_running AS account_type_count_balance_percent_rank,
        CUME_DIST() OVER (ORDER BY balance NULLS FIRST) AS balance_cume_dist,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('month', opened_date) ORDER BY account_type_count_balance NULLS FIRST) AS account_type_count_balance_cume_dist,
        DENSE_RANK() OVER w_grp AS row_seq_2_dense_rank,
        CUME_DIST() OVER w_hist AS account_type_max_row_seq_whole_cume_dist
    FROM metrics
    WINDOW w_running AS (ORDER BY account_type_count_balance DESC NULLS FIRST), w_grp AS (ORDER BY row_seq_2 DESC NULLS FIRST), w_hist AS (ORDER BY account_type_max_row_seq_whole DESC)
)
-- Grouped metrics
SELECT
    CASE WHEN balance_cume_dist >= 0.74 THEN 'LARGE' ELSE 'SMALL' END AS balance_cume_dist_tier,
    APPROX_PERCENTILE(row_seq_2_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS row_seq_2_dense_rank_quartiles,
    PERCENTILE_APPROX(account_type_max_row_seq_whole, 0.5) AS account_type_max_row_seq_whole_p50,
    APPROX_PERCENTILE(balance_cume_dist, 0.95) AS balance_cume_dist_p95,
    ROUND(AVG(balance), 2) AS avg_balance
FROM with_windows
GROUP BY CASE WHEN balance_cume_dist >= 0.74 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY balance_cume_dist_tier DESC;
