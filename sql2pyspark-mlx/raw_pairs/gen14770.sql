WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        is_new_session,
        session_id,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share,
        AVG(session_id) OVER (ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS session_id_avg_2_prior
    FROM sessions_transactions
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_share) OVER (PARTITION BY YEAR(txn_time), merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_share_max_30d,
        LEAD(amount_share, 2) OVER (PARTITION BY YEAR(txn_time), account_id ORDER BY txn_time, id) AS next_amount_share
    FROM enriched
),
enriched_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(session_id) OVER (PARTITION BY YEAR(txn_time), merchant ORDER BY is_new_session, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS session_id_avg_5_prior,
        NTILE(3) OVER (PARTITION BY merchant ORDER BY next_amount_share, id) AS next_amount_share_bucket,
        NTILE(4) OVER w_running AS is_new_session_bucket,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2
    FROM metrics
    WINDOW w_running AS (PARTITION BY MONTH(txn_time), merchant ORDER BY is_new_session, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(is_new_session_bucket, 1, 0) OVER w_recent AS next_is_new_session_bucket,
        FIRST_VALUE(is_new_session_bucket) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY amount_share_max_30d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_new_session_bucket
    FROM enriched_transactions
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY is_new_session_bucket, id)
),
with_windows_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(first_known_is_new_session_bucket, 1) OVER w_grp AS prev_first_known_is_new_session_bucket,
        ROW_NUMBER() OVER w_recent AS event_seq,
        CUME_DIST() OVER (PARTITION BY txn_type ORDER BY session_id_avg_5_prior NULLS FIRST) AS session_id_avg_5_prior_cume_dist
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('YEAR', txn_time), account_id ORDER BY session_id, id), w_recent AS (ORDER BY next_amount_share DESC, id)
)
SELECT
    id,
    account_id,
    txn_type,
    amount,
    merchant,
    session_id,
    next_amount_share_bucket,
    amount_avg_2,
    next_is_new_session_bucket
FROM with_windows_transactions
ORDER BY next_amount_share_bucket DESC;
