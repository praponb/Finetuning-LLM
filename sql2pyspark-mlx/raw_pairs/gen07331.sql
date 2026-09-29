SELECT carrier, cost, LOWER(carrier) AS carrier_lower FROM shipments WHERE carrier IN ('FedEx', 'DHL', 'UPS')
