SELECT YEAR(order_date) AS order_year, MIN(order_date) AS min_order_date FROM orders GROUP BY YEAR(order_date);
