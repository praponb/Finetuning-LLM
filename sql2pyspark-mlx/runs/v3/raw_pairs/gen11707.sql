SELECT id, channel, amount, discount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS rn FROM orders) AS ranked WHERE rn <= 3 ORDER BY 3 DESC, channel
