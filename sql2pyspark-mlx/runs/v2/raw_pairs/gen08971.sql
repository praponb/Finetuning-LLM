WITH Numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rnk FROM suppliers) SELECT id, city, rating, country FROM Numbered WHERE rnk <= 1
