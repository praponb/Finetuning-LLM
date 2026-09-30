SELECT paid_month, status_values, amount_count
FROM (
    SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, COUNT(amount) AS amount_count
    FROM payments
    WHERE paid_at >= '2025-08-01'
    GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM')
) AS s
WHERE amount_count < 8
ORDER BY amount_count DESC, 1 DESC
LIMIT 10
