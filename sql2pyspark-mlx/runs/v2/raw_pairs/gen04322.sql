SELECT * FROM (SELECT MONTH(order_date) AS order_month, MIN(discount) AS min_discount FROM orders WHERE status = 'COMPLETED' GROUP BY MONTH(order_date)) AS x WHERE min_discount < 0.19
