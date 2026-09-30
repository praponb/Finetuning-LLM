WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN method = 'bank_transfer' THEN 1 ELSE 0 END AS is_bank_transfer
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-10-05 16:00:00')
      AND order_id = 12
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id ASC) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        paid_at,
        is_bank_transfer,
        SUM(is_bank_transfer) OVER (PARTITION BY status, order_id ORDER BY rn ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_bank_transfer,
        NTILE(4) OVER (ORDER BY is_bank_transfer DESC, id ASC) AS is_bank_transfer_bucket
    FROM latest
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        cumulative_is_bank_transfer,
        is_bank_transfer_bucket,
        AVG(cumulative_is_bank_transfer) OVER (ORDER BY is_bank_transfer_bucket ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_is_bank_transfer_avg_6,
        CUME_DIST() OVER (ORDER BY cumulative_is_bank_transfer DESC) AS cumulative_is_bank_transfer_cume_dist
    FROM enriched
    WHERE amount <= 640
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_is_bank_transfer_cume_dist >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_is_bank_transfer_cume_dist_tier,
    SUM(is_bank_transfer_bucket) AS total_is_bank_transfer_bucket,
    ROUND(AVG(cumulative_is_bank_transfer), 2) AS avg_cumulative_is_bank_transfer,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(cumulative_is_bank_transfer_cume_dist, 0.9) AS cumulative_is_bank_transfer_cume_dist_p90
FROM scored
GROUP BY 1;
