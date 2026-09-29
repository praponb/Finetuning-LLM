SELECT
    status,
    order_id,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded
FROM payments
WHERE amount > 1230
GROUP BY status, order_id
ORDER BY status DESC, order_id
LIMIT 5;
