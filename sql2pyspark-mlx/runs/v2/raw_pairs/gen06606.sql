SELECT id, delivered_date, order_id, status, DAY(shipped_date) AS shipped_date_day FROM shipments WHERE cost >= 72 AND carrier = 'Flash' ORDER BY status DESC;
