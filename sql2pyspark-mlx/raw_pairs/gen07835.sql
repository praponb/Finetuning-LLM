select id, carrier, cost, status, lower(carrier) as carrier_lower from shipments where cost > 81 and carrier = 'FedEx'
