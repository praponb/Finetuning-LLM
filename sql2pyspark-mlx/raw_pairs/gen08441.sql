SELECT id, order_id, delivered_date, shipped_date, DATE_FORMAT(shipped_date, 'yyyy-MM-dd') shipped_date_label FROM shipments WHERE carrier = 'Flash' ORDER BY shipped_date_label DESC;
