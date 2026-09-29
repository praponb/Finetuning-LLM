SELECT id, cost, shipped_date, DATEDIFF(delivered_date, shipped_date) AS days_shipped_date_to_delivered_date, UPPER(carrier) AS carrier_upper FROM shipments WHERE carrier = 'DHL'
