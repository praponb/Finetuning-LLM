SELECT delivered_date, carrier, status, DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date FROM shipments WHERE cost < 79 ORDER BY days_since_shipped_date DESC
