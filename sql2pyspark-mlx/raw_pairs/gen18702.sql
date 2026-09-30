WITH filtered AS (
    SELECT
        *,
        TO_DATE(txn_time) AS txn_time_date,
        CAST(txn_time AS DATE) AS txn_time_day,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= DATE '2023-04-20'
      AND merchant IS NOT DISTINCT FROM 'Amazon'
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY merchant, txn_type ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share
    FROM filtered
    WHERE txn_time_date > '2025-02-01'
),
with_dims AS (
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.amount,
        s.txn_time,
        s.merchant,
        s.resolved_merchant,
        s.amount_share,
        b.balance,
        b.opened_date,
        b.status
    FROM enriched s
    LEFT JOIN accounts b
        ON s.account_id = b.id
),
enriched_transactions AS (
    SELECT
        a.*,
        b.opened_date AS account_opened_date,
        b.account_type,
        b.status AS account_status
    FROM with_dims a
    JOIN accounts b
        ON a.account_id = b.id
),
flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_status ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_status ORDER BY txn_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched_transactions
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
kept_keys AS (
    SELECT id, merchant
    FROM islands
    INTERSECT
    SELECT id, merchant
    FROM transactions
    WHERE txn_type = 'FEE'
),
kept_rows AS (
    SELECT s.*
    FROM islands s
    JOIN kept_keys k ON s.id = k.id AND s.merchant = k.merchant
)
SELECT
    p.account_type AS account_account_type,
    a.status AS transaction_status,
    COUNT(*) AS cnt,
    MAX(a.balance) AS max_balance,
    SUM(a.amount) AS total_amount
FROM kept_rows a
JOIN accounts p ON a.account_id = p.id
GROUP BY ROLLUP (p.account_type, a.status)
ORDER BY total_amount DESC
