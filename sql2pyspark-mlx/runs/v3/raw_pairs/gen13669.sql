SELECT
    YEAR(delivered_date) AS delivered_year,
    carrier,
    COUNT(DISTINCT cost) AS unique_orders,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    MAX(cost) AS max_cost
FROM shipments
GROUP BY YEAR(delivered_date), carrier
ORDER BY stddev_cost DESC, delivered_year, carrier
LIMIT 20
