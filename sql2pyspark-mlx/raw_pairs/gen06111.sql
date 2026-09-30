SELECT QUARTER(shipped_date) AS shipped_quarter, status, MIN(delivered_date) AS min_delivered_date FROM shipments WHERE cost < 38 GROUP BY 1, 2
