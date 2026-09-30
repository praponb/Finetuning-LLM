SELECT
    CASE WHEN status IN ('RETURNED', 'COMPLETED') THEN 'core' ELSE 'other' END status_group,
    MAX(amount) - MIN(amount) amount_range,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) channel_values,
    MAX(discount) - MIN(discount) discount_range
FROM orders
GROUP BY 1
ORDER BY discount_range DESC;
