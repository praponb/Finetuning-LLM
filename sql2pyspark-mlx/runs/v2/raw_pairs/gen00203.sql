SELECT id, channel, discount, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) AS rn FROM orders) AS t WHERE rn <= 3 ORDER BY id DESC
