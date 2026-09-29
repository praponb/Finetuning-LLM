WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN amount >= 3800 THEN 'High' WHEN amount >= 1100 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        CASE WHEN amount >= 4200 THEN 'High' WHEN amount >= 1200 THEN 'Medium' ELSE 'Low' END AS amount_level
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-09-17 17:00:00'
      AND merchant IS NOT NULL
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        b.opened_date,
        b.account_type
    FROM valid s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY txn_time DESC, id ASC) AS seq_no,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY YEAR(opened_date), merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY account_id ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM with_dims
),
enriched_transactions AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_time,
        is_flagged,
        amount_level,
        account_type,
        seq_no,
        last_known_merchant,
        AVG(row_seq) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS row_seq_avg_7d,
        NTILE(5) OVER w_hist AS row_seq_bucket
    FROM enriched
    WINDOW w_hist AS (ORDER BY row_seq ASC, id ASC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_running AS seq_no_rank,
        DENSE_RANK() OVER (ORDER BY seq_no DESC) AS seq_no_dense_rank
    FROM enriched_transactions
    WHERE row_seq_bucket <= 8
    WINDOW w_running AS (PARTITION BY TO_DATE(txn_time), account_type ORDER BY seq_no DESC NULLS LAST)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, account_id
    FROM scored
    EXCEPT
    SELECT id, account_id
    FROM transactions
    WHERE merchant = 'Starbucks'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.id = k.id AND s.account_id = k.account_id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM verified
    WHERE account_type IS NOT NULL
)
SELECT
    is_flagged,
    amount_level,
    account_type,
    last_known_merchant,
    row_seq_avg_7d,
    row_seq_bucket,
    seq_no_rank,
    seq_no_dense_rank
FROM filtered
ORDER BY row_seq_avg_7d ASC
