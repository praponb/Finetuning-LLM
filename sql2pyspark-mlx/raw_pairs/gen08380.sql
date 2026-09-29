SELECT id, status, shipped_date, DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_date_label FROM shipments
