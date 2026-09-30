SELECT
    status,
    SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) fedex_count,
    ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_ups,
    SORT_ARRAY(COLLECT_SET(carrier)) carrier_list
FROM shipments
WHERE YEAR(shipped_date) = 2025
GROUP BY status
ORDER BY status
