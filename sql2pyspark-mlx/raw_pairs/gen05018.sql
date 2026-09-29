SELECT id, order_id, LAST_DAY(shipped_date) shipped_date_month_end FROM shipments ORDER BY order_id DESC, id LIMIT 50
