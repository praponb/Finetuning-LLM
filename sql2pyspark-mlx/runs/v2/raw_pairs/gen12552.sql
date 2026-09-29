SELECT id, discount, order_date, status, DATE_FORMAT(order_date, 'yyyy-MM') AS order_date_label, ROUND(amount, 0) AS amount_rounded FROM orders ORDER BY status DESC;
