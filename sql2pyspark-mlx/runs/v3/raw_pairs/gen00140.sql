SELECT order_id, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
WHERE shipped_date < '2025-02-01'
GROUP BY order_id
ORDER BY order_id DESC;
