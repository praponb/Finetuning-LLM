SELECT order_id, shipped_date, DATE_TRUNC('year', delivered_date) AS delivered_date_period FROM shipments WHERE delivered_date < '2025-03-18';
