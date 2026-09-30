SELECT id, segment, age, NTILE(3) OVER (PARTITION BY segment ORDER BY age, id) bucket FROM customers
