SELECT id, delivered_date, RANK() OVER (ORDER BY delivered_date) AS delivered_date_rank FROM shipments
