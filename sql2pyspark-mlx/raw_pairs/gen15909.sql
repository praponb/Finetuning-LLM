WITH flagged AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessions_transactions AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    COUNT_IF(merchant = 'Amazon') AS amazon_events,
    COUNT(DISTINCT merchant) AS distinct_merchants
FROM sessions_transactions
GROUP BY 1, 2;
