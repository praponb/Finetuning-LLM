SELECT
    order_id,
    status,
    ROUND(STDDEV(cost), 2) stddev_cost,
    SUM(cost) total_cost
FROM shipments
WHERE shipped_date >= '2025-10-01'
GROUP BY order_id, status
HAVING ROUND(STDDEV(cost), 2) >= 41;
