SELECT
    DATE_TRUNC('MONTH', delivered_date) delivered_month,
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) carrier_values,
    SORT_ARRAY(COLLECT_SET(carrier)) carrier_list
FROM shipments
WHERE delivered_date > '2025-04-17'
GROUP BY 1, 2
ORDER BY carrier_values
