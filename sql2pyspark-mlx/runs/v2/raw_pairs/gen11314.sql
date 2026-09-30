SELECT channel, MAX(discount) AS max_discount FROM orders GROUP BY channel ORDER BY max_discount;
