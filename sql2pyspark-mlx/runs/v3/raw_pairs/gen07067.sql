SELECT *
FROM (
    SELECT QUARTER(delivered_date) AS delivered_quarter, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM shipments
    GROUP BY QUARTER(delivered_date)
) AS s
WHERE stddev_cost >= 49
ORDER BY stddev_cost DESC, delivered_quarter
LIMIT 3
