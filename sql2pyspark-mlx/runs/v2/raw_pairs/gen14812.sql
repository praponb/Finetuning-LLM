SELECT
    carrier,
    order_id,
    ROUND(AVG(cost), 2) AS avg_cost,
    MAX(cost) AS max_cost
FROM shipments
WHERE YEAR(delivered_date) = 2023 AND order_id IS NULL
GROUP BY carrier, order_id
HAVING avg_cost < 54
ORDER BY order_id
