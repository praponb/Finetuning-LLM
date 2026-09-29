SELECT id, customer_id, channel, discount, SUM(discount) OVER (PARTITION BY customer_id, channel) AS customer_id_sum_discount FROM orders ORDER BY 5 DESC;
