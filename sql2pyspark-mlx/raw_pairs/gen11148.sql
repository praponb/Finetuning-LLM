WITH with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.discount,
        p.amount AS order_amount
    FROM payments s
    JOIN orders p
        ON s.order_id <=> p.id
    WHERE p.channel IN ('store', 'partner', 'web')
)
-- Grouped metrics
SELECT
    CASE WHEN amount >= 1690 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    method,
    ROUND(AVG(amount), 2) AS avg_amount,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    MAX(discount) FILTER (WHERE method = 'cash') AS cash_max_discount,
    SIZE(COLLECT_SET(method)) AS num_methods
FROM with_dims
GROUP BY GROUPING SETS ((CASE WHEN amount >= 1690 THEN 'PREMIUM' ELSE 'STANDARD' END, method), (CASE WHEN amount >= 1690 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY avg_amount DESC NULLS FIRST;
