SELECT amount, UPPER(status) AS status_upper FROM payments WHERE paid_at < '2023-03-20' AND order_id IN (2, 10) ORDER BY amount ASC;
