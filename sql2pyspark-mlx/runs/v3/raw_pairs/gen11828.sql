SELECT *, TRIM(status) AS clean_status FROM orders WHERE MONTH(order_date) IN (7, 8, 9) AND status != 'RETURNED' ORDER BY amount, customer_id DESC
