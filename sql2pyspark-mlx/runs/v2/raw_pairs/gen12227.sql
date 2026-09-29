SELECT *, DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date, LOWER(carrier) AS carrier_lower
FROM shipments
