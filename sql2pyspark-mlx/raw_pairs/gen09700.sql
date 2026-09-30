SELECT id, carrier, cost, ROUND(DATEDIFF(shipped_date, delivered_date) / 30, 1) AS months_delivered_date_to_shipped_date FROM shipments WHERE MONTH(shipped_date) IN (1, 4, 6);
