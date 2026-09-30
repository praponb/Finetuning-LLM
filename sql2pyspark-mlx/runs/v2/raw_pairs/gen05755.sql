WITH Ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS rn FROM orders) SELECT id, channel, amount, store_id FROM Ranked WHERE rn <= 3
