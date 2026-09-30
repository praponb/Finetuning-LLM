WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_time,
        merchant,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_amount,
        MIN(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_amount,
        MAX(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_amount,
        COUNT(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d
    FROM transactions
    WHERE amount >= 2100
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(is_new_session) OVER w_grp AS last3_sum_is_new_session,
        AVG(is_new_session) OVER w_grp AS last3_avg_is_new_session,
        MAX(is_new_session) OVER w_grp AS last3_max_is_new_session
    FROM sessions_transactions
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY last3_sum_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY last3_sum_is_new_session DESC NULLS FIRST, id) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY last3_sum_amount DESC, id) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        session_id / NULLIF(last3_sum_is_new_session, 0) AS session_id_per_last3_sum_is_new_session,
        CEIL(last3_sum_amount) AS last3_sum_amount_whole,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM latest
    WHERE merchant IS NOT NULL
      AND last3_min_amount > 3800
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_time,
        merchant,
        last3_sum_amount,
        last3_min_amount,
        amount_count_30d,
        is_new_session,
        session_id,
        last3_avg_is_new_session,
        rank_in_group,
        session_id_per_last3_sum_is_new_session,
        merchant_clean,
        LAG(session_id_per_last3_sum_is_new_session) OVER w_ordered AS prev_session_id_per_last3_sum_is_new_session,
        LEAD(last3_sum_amount) OVER w_ordered AS next_last3_sum_amount,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM valid
    WINDOW w_ordered AS (PARTITION BY merchant ORDER BY last3_sum_amount, id)
)
-- Aggregate with conditional metrics
SELECT
    merchant,
    APPROX_PERCENTILE(last3_sum_amount, ARRAY(0.25, 0.5, 0.75)) AS last3_sum_amount_quartiles,
    COUNT_IF(merchant = '7-Eleven') AS 7_eleven_events,
    COUNT(*) FILTER (WHERE merchant = 'Lazada') AS lazada_rows,
    COUNT_IF(merchant = 'Starbucks') AS starbucks_events
FROM scored
GROUP BY merchant
