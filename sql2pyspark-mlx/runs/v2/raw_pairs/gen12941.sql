SELECT id, status, shipped_date, LAST_DAY(shipped_date) AS shipped_date_month_end FROM shipments WHERE order_id IS NULL ORDER BY id DESC LIMIT 10;
