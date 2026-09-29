WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS latest_rank
        FROM payments
    ) t
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    status,
    MAX(paid_at) AS last_paid_at,
    STDDEV_SAMP(amount) AS amount_stddev,
    PERCENTILE_APPROX(amount, 0.5) AS amount_p50,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS amount_per_success
FROM ranked
GROUP BY status
ORDER BY amount_stddev;
