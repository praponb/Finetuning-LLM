WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM transactions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM islands
    INTERSECT
    SELECT id
    FROM transactions
    WHERE is_flagged
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    INNER JOIN kept_keys k ON s.id = k.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.account_id,
        src.amount,
        src.is_flagged,
        src.session_id,
        d.balance,
        d.status
    FROM reconciled src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
scored AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (ORDER BY balance) AS balance_rank
    FROM joined
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        session_id,
        balance,
        balance_rank,
        STDDEV_SAMP(balance_rank) OVER (PARTITION BY account_id, status) AS account_id_stddev_samp_balance_rank,
        STDDEV_SAMP(balance) OVER (PARTITION BY status) AS status_stddev_samp_balance,
        CUME_DIST() OVER (ORDER BY balance_rank DESC NULLS FIRST) AS balance_rank_cume_dist
    FROM scored
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        balance,
        balance_rank,
        FLOOR(balance_rank_cume_dist) AS balance_rank_cume_dist_whole
    FROM metrics
    WHERE session_id BETWEEN 4 AND 5
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_rank_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(balance_rank, ARRAY(0.25, 0.5, 0.75)) AS balance_rank_quartiles,
    SUM(balance_rank_cume_dist_whole) AS total_balance_rank_cume_dist_whole
FROM base
GROUP BY CASE WHEN balance_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(*) > 1
ORDER BY cnt NULLS LAST;
