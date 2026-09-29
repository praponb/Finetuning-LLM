SELECT
    QUARTER(shipped_date) AS shipped_quarter,
    carrier,
    MAX(cost) - MIN(cost) AS cost_range,
    AVG(cost) AS avg_cost
FROM shipments
WHERE carrier IN ('DHL', 'Flash', 'FedEx') AND carrier <> 'FedEx'
GROUP BY QUARTER(shipped_date), carrier
HAVING AVG(cost) < 54
ORDER BY cost_range, shipped_quarter, carrier
LIMIT 10
