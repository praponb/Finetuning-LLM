WITH deduped AS (
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id ASC) AS dedup_rank
        FROM transactions
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    CASE WHEN amount >= 4100 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    merchant,
    COUNT(*) AS cnt,
    MAX(txn_time) AS last_txn_time,
    COUNT_IF(merchant = 'Shell') AS shell_events,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    COUNT(CASE WHEN merchant = 'Amazon' THEN 1 END) AS amazon_count,
    PERCENTILE_APPROX(amount, 0.95) AS amount_p95,
    RANK() OVER (PARTITION BY CASE WHEN amount >= 4100 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(merchant = 'Shell') DESC) AS shell_events_rank
FROM deduped
GROUP BY CASE WHEN amount >= 4100 THEN 'HIGH' ELSE 'LOW' END, merchant
ORDER BY cnt ASC
