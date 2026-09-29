WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TO_DATE('2024-04-03')
      AND merchant IS NOT NULL
      AND is_flagged = TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, merchant
    FROM cleaned
    EXCEPT ALL
    SELECT id, merchant
    FROM transactions
    WHERE is_flagged
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.merchant = k.merchant
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_flagged_flag,
        resolved_merchant,
        MAX(is_flagged_flag) OVER w_grp AS merchant_max_is_flagged_flag,
        DENSE_RANK() OVER (PARTITION BY account_id ORDER BY is_flagged_flag DESC) AS is_flagged_flag_dense_rank,
        MIN(is_flagged_flag) OVER w_grp AS merchant_min_is_flagged_flag
    FROM reconciled
    WINDOW w_grp AS (PARTITION BY merchant)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT e.*, b.opened_date
    FROM scored e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        d.account_type,
        d.status
    FROM with_dims s
    LEFT JOIN accounts d
        ON s.account_id = d.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        b.balance,
        b.status AS account_status,
        b.opened_date AS account_opened_date
    FROM with_dims_transactions e
    LEFT JOIN accounts b
        ON e.account_id = b.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    merchant,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(is_flagged_flag_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_flag_dense_rank_quartiles,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE txn_type = 'DEBIT') AS debit_rows,
    COUNT_IF(account_status = 'FROZEN') AS frozen_events,
    GROUPING(DATE_TRUNC('QUARTER', txn_time)) AS gid
FROM joined
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', txn_time), merchant, DATE_TRUNC('YEAR', txn_time))
