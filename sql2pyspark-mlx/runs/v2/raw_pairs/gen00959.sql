SELECT
    order_id,
    SUM(cost) total_cost,
    COUNT(*) cnt,
    SUM(SUM(cost)) OVER (ORDER BY order_id) AS cumulative_cost
FROM shipments
GROUP BY order_id
ORDER BY order_id DESC
