SELECT id, delivered_date, cost, status, DATE_ADD(shipped_date, 78) AS shipped_date_plus_78d FROM shipments WHERE status = 'LOST' ORDER BY id DESC, 4
