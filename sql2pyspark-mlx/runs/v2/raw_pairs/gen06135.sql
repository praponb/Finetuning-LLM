SELECT
    DATE_FORMAT(shipped_date, 'yyyy-MM') shipped_month,
    COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) dhl_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) status_values,
    COUNT(*) shipment_count
FROM shipments
WHERE (order_id IN (33, 40) AND carrier = 'Kerry') OR shipped_date >= '2024-06-23'
GROUP BY DATE_FORMAT(shipped_date, 'yyyy-MM');
