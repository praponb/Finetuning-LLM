WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        DATE_TRUNC('month', txn_time) AS txn_time_period,
        COALESCE(merchant, 'NONE') AS merchant_clean
    FROM transactions
    WHERE txn_time > DATE '2024-04-21'
      AND merchant IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share_2,
        NTILE(5) OVER (PARTITION BY account_id, txn_type ORDER BY amount DESC, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY account_id, merchant ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        NTILE(3) OVER w_recent AS amount_bucket_2
    FROM filtered
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY amount DESC, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount_bucket / SUM(amount_bucket) OVER (PARTITION BY txn_type), 4) AS amount_bucket_share,
        NTILE(3) OVER (PARTITION BY account_id, merchant ORDER BY amount_share_2 DESC, id) AS amount_share_2_bucket,
        COUNT(cumulative_amount) OVER (PARTITION BY merchant, account_id) AS merchant_count_cumulative_amount,
        ROUND(cumulative_amount / SUM(cumulative_amount) OVER (PARTITION BY account_id), 4) AS cumulative_amount_share,
        SUM(amount_bucket) OVER (PARTITION BY merchant ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_bucket
    FROM scored
)
-- Grouped metrics
SELECT
    merchant,
    MIN(txn_time_period) AS started_at,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    APPROX_PERCENTILE(cumulative_amount_share, 0.5) AS cumulative_amount_share_p50,
    COUNT(*) AS event_count
FROM enriched
GROUP BY 1
ORDER BY merchant DESC NULLS LAST
