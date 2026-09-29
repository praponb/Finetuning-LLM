WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM transactions
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_time,
        merchant,
        SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Grouped metrics
SELECT
    CASE WHEN island_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS island_id_tier,
    PERCENTILE_APPROX(island_id, ARRAY(0.25, 0.5, 0.75)) AS island_id_quartiles,
    MAX(island_id) FILTER (WHERE merchant = 'Amazon') AS amazon_max_island_id,
    PERCENTILE_APPROX(island_id, 0.5) AS island_id_p50,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    MAX(txn_time) AS ended_at,
    SUM(CASE WHEN merchant = '7-Eleven' THEN island_id ELSE 0 END) AS 7_eleven_island_id
FROM sessions_transactions
GROUP BY 1
HAVING 7_eleven_island_id > 0
ORDER BY amazon_max_island_id DESC NULLS FIRST
