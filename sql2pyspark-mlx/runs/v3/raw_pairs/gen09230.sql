WITH ranked_orders AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS pos FROM orders) SELECT id, channel, amount, discount FROM ranked_orders WHERE pos = 1
