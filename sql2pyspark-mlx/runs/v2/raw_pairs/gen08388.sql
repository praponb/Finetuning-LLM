SELECT
    HOUR(paid_at) AS paid_hour,
    MAX(amount) - MIN(amount) AS amount_range,
    SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM payments
WHERE status = 'REFUNDED'
GROUP BY paid_hour
HAVING SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) > 44;
