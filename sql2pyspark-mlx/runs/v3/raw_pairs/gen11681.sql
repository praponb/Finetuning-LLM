SELECT id, rating, ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS rn FROM suppliers
