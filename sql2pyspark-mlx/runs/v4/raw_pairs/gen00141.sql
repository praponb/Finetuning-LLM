WITH valid AS (
    SELECT
        *,
        TRIM(txn_type) AS clean_txn_type,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time >= '2024-04-01'
)
SELECT
    *,
    NTILE(4) OVER (PARTITION BY account_id ORDER BY amount ASC, id ASC) AS amount_bucket,
    SUM(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type, merchant ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    COUNT(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d,
    DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount ASC) AS amount_dense_rank
FROM valid
