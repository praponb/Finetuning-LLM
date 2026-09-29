SELECT
    order_id,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    MAX(paid_at) AS last_paid_at
FROM payments
GROUP BY 1
