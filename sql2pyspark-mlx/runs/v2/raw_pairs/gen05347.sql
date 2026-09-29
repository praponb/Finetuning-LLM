SELECT id, delivered_date, carrier, status FROM shipments WHERE order_id IS NULL AND carrier IN ('Kerry', 'FedEx')
