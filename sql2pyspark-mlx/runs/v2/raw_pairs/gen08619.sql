SELECT
    order_id,
    QUARTER(shipped_date) AS shipped_quarter,
    MAX(cost) - MIN(cost) AS cost_range,
    ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY order_id, QUARTER(shipped_date)
HAVING stddev_cost < 56;
