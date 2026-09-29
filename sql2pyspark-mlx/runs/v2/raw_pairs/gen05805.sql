SELECT order_id, shipped_date, carrier, LENGTH(status) AS status_length FROM shipments WHERE status NOT IN ('IN_TRANSIT', 'RETURNED')
