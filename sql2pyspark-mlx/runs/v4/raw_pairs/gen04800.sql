SELECT id, store_id, status, LAST_DAY(order_date) AS order_date_month_end FROM orders ORDER BY order_date_month_end DESC, store_id DESC, id LIMIT 5;
