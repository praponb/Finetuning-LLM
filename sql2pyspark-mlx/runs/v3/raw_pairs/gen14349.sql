SELECT order_id, status, MIN(shipped_date) AS min_shipped_date
FROM shipments
GROUP BY ROLLUP(order_id, status)
