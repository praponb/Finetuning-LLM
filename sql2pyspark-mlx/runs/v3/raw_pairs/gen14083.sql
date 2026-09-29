SELECT carrier, MIN(shipped_date) AS first_shipped_date, ROUND(STDDEV(cost), 2) AS stddev_cost FROM shipments WHERE cost > 80 GROUP BY 1 HAVING stddev_cost > 63
