SELECT id, rating, ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS rn FROM suppliers WHERE city NOT IN ('New York', 'London', 'Singapore')
