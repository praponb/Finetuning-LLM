select id, carrier, order_id from shipments where carrier = 'FedEx' union select id, carrier, order_id from shipments where shipped_date >= '2025-07-01' order by order_id
