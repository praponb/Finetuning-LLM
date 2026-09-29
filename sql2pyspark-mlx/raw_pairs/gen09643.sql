SELECT *, LEAST(amount, discount) AS least_amount_discount, DATE_FORMAT(order_date, 'yyyy-MM-dd') AS order_date_label
FROM orders
ORDER BY id ASC
