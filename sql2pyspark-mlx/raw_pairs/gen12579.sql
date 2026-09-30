SELECT
    status,
    carrier,
    MONTH(shipped_date) AS shipped_month,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    COUNT(cost) AS cost_count,
    COUNT(*) AS num_shipments
FROM shipments
WHERE delivered_date BETWEEN '2024-06-10' AND '2024-07-01' AND delivered_date <= '2024-12-24' AND shipped_date < '2024-08-22'
GROUP BY status, carrier, shipped_month
HAVING stddev_cost >= 34;
