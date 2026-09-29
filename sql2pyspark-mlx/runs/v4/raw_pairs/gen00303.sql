WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY txn_time, id) event_seq,
        AVG(amount) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) amount_avg_2_prior
    FROM transactions
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(merchant) merchant_length,
        COALESCE(merchant, 'UNKNOWN') merchant_clean
    FROM scored
    WHERE merchant IS NOT NULL
      AND account_id IN (10, 17)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY event_seq DESC) AS rank_in_group
        FROM islands
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY session_id DESC, id) AS row_num
        FROM ranked
    ) t
    WHERE row_num = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        event_seq,
        amount_avg_2_prior,
        merchant_length,
        merchant_clean,
        is_new_burst,
        session_id,
        rank_in_group,
        row_num,
        SUM(is_new_burst) OVER w_running account_id_sum_is_new_burst,
        CUME_DIST() OVER w_ordered amount_avg_2_prior_cume_dist,
        AVG(event_seq) OVER (ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) event_seq_avg_3
    FROM latest
    WINDOW w_running AS (PARTITION BY account_id), w_ordered AS (ORDER BY amount_avg_2_prior DESC)
),
valid_transactions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        amount_avg_2_prior + is_new_burst amount_avg_2_prior_plus_is_new_burst,
        COALESCE(merchant, 'NONE') merchant_clean_2
    FROM metrics
    WHERE is_new_burst BETWEEN 0 AND 1
)
SELECT
    id,
    account_id,
    txn_type,
    merchant,
    is_flagged,
    event_seq,
    amount_avg_2_prior,
    merchant_length,
    is_new_burst,
    CASE WHEN merchant RLIKE '^(7-Eleven|Lazada)$' THEN 'PRIORITY' WHEN merchant RLIKE '^(Grab|Amazon)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM valid_transactions
