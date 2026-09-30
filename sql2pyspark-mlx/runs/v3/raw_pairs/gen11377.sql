SELECT id, channel, amount, discount, FIRST_VALUE(discount) OVER (PARTITION BY channel ORDER BY amount DESC, id) AS first_discount FROM orders ORDER BY id ASC
