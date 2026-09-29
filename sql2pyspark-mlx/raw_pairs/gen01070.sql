SELECT
    carrier,
    DATE_FORMAT(delivered_date, 'yyyy-MM') delivered_month,
    ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_ups,
    ROUND((100 * SUM(CASE WHEN carrier = 'DHL' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_dhl
FROM shipments
WHERE status <> 'DELIVERED' OR cost > 63
GROUP BY ROLLUP(carrier, DATE_FORMAT(delivered_date, 'yyyy-MM'));
