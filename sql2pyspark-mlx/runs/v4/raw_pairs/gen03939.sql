SELECT
    order_id,
    status,
    MONTH(shipped_date) AS shipped_month,
    MAX(cost) - MIN(cost) AS cost_range
FROM shipments
GROUP BY order_id, status, MONTH(shipped_date)
HAVING MAX(cost) - MIN(cost) < 42
ORDER BY shipped_month DESC, order_id, status
LIMIT 10;
