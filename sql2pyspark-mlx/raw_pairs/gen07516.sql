SELECT *, UPPER(status) AS status_upper FROM shipments WHERE carrier = 'Flash' ORDER BY order_id, status_upper
