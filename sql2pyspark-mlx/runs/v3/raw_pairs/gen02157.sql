SELECT QUARTER(order_date) AS order_quarter, channel, COUNT(discount) AS discount_count, AVG(discount) AS avg_discount FROM orders GROUP BY QUARTER(order_date), channel
