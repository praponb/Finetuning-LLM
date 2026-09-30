SELECT shipped_date FROM shipments WHERE carrier IN ('FedEx', 'Flash') UNION ALL SELECT order_date FROM orders WHERE customer_id IS NOT NULL
