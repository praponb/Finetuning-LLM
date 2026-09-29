WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER w_part AS last7_sum_amount,
        MAX(amount) OVER w_part AS last7_max_amount,
        AVG(amount) OVER w_part AS last7_avg_amount
    FROM transactions
    WINDOW w_part AS (PARTITION BY merchant, account_id ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        last7_sum_amount,
        last7_max_amount,
        last7_avg_amount,
        LOWER(merchant) AS merchant_lower,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM scored
    WHERE txn_time > TIMESTAMP '2023-11-10 03:00:00'
      AND merchant IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount) OVER w_running AS next_amount,
        LAG(amount) OVER w_running AS prev_amount,
        LAG(last7_avg_amount) OVER w_running AS prev_last7_avg_amount,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM base
    WINDOW w_running AS (PARTITION BY merchant ORDER BY last7_avg_amount, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        amount,
        txn_time,
        next_amount,
        row_seq
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time NULLS LAST, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        next_amount,
        row_seq,
        NTILE(10) OVER (PARTITION BY MONTH(txn_time) ORDER BY row_seq DESC, id) AS row_seq_bucket,
        LAG(amount, 1) OVER w_hist AS prev_amount
    FROM ranked
    WINDOW w_hist AS (ORDER BY row_seq, id)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM transactions
    WHERE txn_type <> 'FEE'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM kept_rows
    EXCEPT
    SELECT id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 12 AND 17
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    amount,
    next_amount,
    row_seq_bucket
FROM verified
ORDER BY amount DESC NULLS FIRST, next_amount;
