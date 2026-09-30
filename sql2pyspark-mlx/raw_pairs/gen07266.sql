SELECT id, store_id, DATE_FORMAT(order_date, 'MMM yyyy') AS order_date_label FROM orders WHERE customer_id IN (21, 23, 29) AND (store_id IS NULL OR store_id IN (7, 15, 24, 29))
