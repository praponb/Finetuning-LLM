SELECT
    status,
    order_id,
    MONTH(shipped_date) AS shipped_month,
    AVG(cost) AS avg_cost,
    SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) AS flash_cost
FROM shipments
GROUP BY status, order_id, shipped_month
HAVING MAX(cost) >= 55
ORDER BY shipped_month DESC, status, order_id
LIMIT 3;
