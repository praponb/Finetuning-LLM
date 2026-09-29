WITH flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessions_transactions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
valid AS (
    SELECT *, amount / NULLIF(session_id, 0) AS amount_per_session_id
    FROM sessions_transactions
    WHERE txn_time >= DATE '2024-02-23'
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY is_new_burst, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
with_windows AS (
    SELECT
        *,
        SUM(session_id) OVER (PARTITION BY txn_type, account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_session_id,
        SUM(amount_per_session_id) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_per_session_id_sum_30d
    FROM deduped
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.merchant,
    s.is_flagged,
    s.is_new_burst,
    s.session_id,
    s.amount_per_session_id,
    CASE WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS merchant_zone,
    (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS min_transaction_amount
FROM with_windows s;
