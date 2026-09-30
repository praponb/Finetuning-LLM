WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS dedup_rank
    FROM transactions
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END AS is_lazada,
        LOWER(merchant) AS merchant_lower
    FROM latest
    WHERE txn_time >= DATE '2023-04-20'
      AND merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.txn_time,
        a.merchant,
        a.is_flagged,
        a.is_lazada,
        a.merchant_lower,
        a.new_group_flag,
        d.opened_date
    FROM sessionized a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(is_lazada) OVER (PARTITION BY TO_DATE(txn_time), account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_lazada_sum_7d,
        NTILE(4) OVER (PARTITION BY txn_type ORDER BY new_group_flag, id) AS new_group_flag_bucket
    FROM with_dims
)
-- Join a dimension and aggregate
SELECT
    dim.account_type,
    e.txn_type,
    COUNT(*) AS num_rows,
    MIN(e.new_group_flag_bucket) AS min_new_group_flag_bucket,
    GROUPING_ID() AS level_id
FROM scored e
JOIN accounts dim ON e.account_id = dim.id
GROUP BY GROUPING SETS ((dim.account_type, e.txn_type), (dim.account_type));
