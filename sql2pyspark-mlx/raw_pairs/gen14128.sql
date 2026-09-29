WITH cleaned AS (
    SELECT
        *,
        LENGTH(method) AS method_length,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE paid_at >= TO_DATE('2023-10-18')
      AND order_id IN (9, 19)
)
SELECT
    CASE WHEN method_length >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END AS method_length_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    AVG(method_length) FILTER (WHERE status = 'FAILED') AS failed_avg_method_length
FROM cleaned
GROUP BY CASE WHEN method_length >= 65 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', paid_at)
HAVING (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 < 1000000
ORDER BY method_length_tier ASC;
