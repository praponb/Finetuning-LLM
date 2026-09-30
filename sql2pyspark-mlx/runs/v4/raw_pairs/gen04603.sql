WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(amount, 2) OVER (PARTITION BY method ORDER BY amount ASC, id ASC) prev_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_amount,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY amount DESC, id ASC) amount_bucket
    FROM payments
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    COUNT(DISTINCT method) AS distinct_methods,
    STDDEV_SAMP(amount) AS amount_stddev,
    AVG(amount_bucket) AS avg_amount_bucket,
    SUM(CASE WHEN method = 'cash' THEN amount_bucket ELSE 0 END) AS cash_amount_bucket,
    AVG(prev_amount) AS avg_prev_amount,
    GROUPING_ID() AS gid
FROM with_windows
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', paid_at))
ORDER BY amount_stddev ASC NULLS FIRST
