SELECT
    DATE_TRUNC('MONTH', delivered_date) AS delivered_month,
    CASE WHEN carrier IN ('UPS', 'DHL') THEN 'group_a' ELSE 'other' END AS carrier_group,
    ROUND(STDDEV(cost), 2) AS stddev_cost,
    SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost
FROM shipments
GROUP BY 1, 2
