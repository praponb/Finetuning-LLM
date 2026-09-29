SELECT CASE WHEN carrier IN ('UPS', 'Flash') THEN 'tier_1' ELSE 'other' END AS carrier_group, carrier, ROUND(STDDEV(cost), 2) AS stddev_cost
FROM shipments
GROUP BY 1, 2
