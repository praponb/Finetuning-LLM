WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        LAG(amount) OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC) AS row_seq,
        NTILE(10) OVER (ORDER BY amount DESC, id ASC) AS amount_bucket
    FROM transactions
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_time,
        merchant,
        next_amount,
        row_seq,
        amount_bucket,
        SUM(next_amount) OVER w_running AS running_sum_next_amount,
        MAX(next_amount) OVER w_running AS running_max_next_amount,
        AVG(next_amount) OVER w_running AS running_avg_next_amount,
        MIN(next_amount) OVER w_running AS running_min_next_amount,
        LAST_VALUE(next_amount) IGNORE NULLS OVER (ORDER BY row_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_amount
    FROM enriched
    WHERE next_amount < 1000
    WINDOW w_running AS (PARTITION BY merchant, txn_type ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        merchant,
        next_amount,
        row_seq,
        running_avg_next_amount,
        running_min_next_amount,
        ROUND(running_max_next_amount / running_min_next_amount, 2) AS running_max_next_amount_per_running_min_next_amount,
        ROUND(running_sum_next_amount / last_known_next_amount, 2) AS running_sum_next_amount_per_last_known_next_amount,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM scored
    WHERE txn_time >= TO_TIMESTAMP('2023-04-16 12:00:00')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY running_avg_next_amount ASC, id ASC) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_sum_next_amount_per_last_known_next_amount >= 79 THEN 'HIGH' ELSE 'LOW' END AS running_sum_next_amount_per_last_known_next_amount_tier,
    merchant,
    ROUND(AVG(next_amount), 2) AS avg_next_amount,
    SUM(CASE WHEN merchant = 'Amazon' THEN running_min_next_amount ELSE 0 END) AS amazon_running_min_next_amount,
    GROUPING(CASE WHEN running_sum_next_amount_per_last_known_next_amount >= 79 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM latest
GROUP BY CUBE (CASE WHEN running_sum_next_amount_per_last_known_next_amount >= 79 THEN 'HIGH' ELSE 'LOW' END, merchant)
HAVING SUM(CASE WHEN merchant = 'Amazon' THEN running_min_next_amount ELSE 0 END) < 1000000
ORDER BY merchant ASC NULLS FIRST;
