SELECT id, store_id, MONTH(order_date) AS order_date_month FROM orders ORDER BY order_date_month DESC, store_id DESC;
