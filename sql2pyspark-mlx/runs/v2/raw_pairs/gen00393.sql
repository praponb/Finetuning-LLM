SELECT id, delivered_date, shipped_date, order_id, DATE_TRUNC('year', shipped_date) AS shipped_date_period FROM shipments WHERE shipped_date <= '2024-03-11';
