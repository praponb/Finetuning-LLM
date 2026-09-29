SELECT order_id, status, refunded_count
FROM (
    SELECT order_id, status, SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) refunded_count
    FROM payments
    WHERE method = 'cash'
    GROUP BY order_id, status
) s
WHERE refunded_count < 49
ORDER BY status;
