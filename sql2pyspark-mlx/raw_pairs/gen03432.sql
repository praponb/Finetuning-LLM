WITH order_summary AS (
    SELECT order_id, ROUND(STDDEV(cost), 2) stddev_cost
    FROM shipments
    WHERE carrier NOT IN ('FedEx', 'Kerry')
    GROUP BY order_id
)
SELECT o.id, o.amount, os.stddev_cost
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id
WHERE os.stddev_cost >= 78
ORDER BY os.stddev_cost DESC
