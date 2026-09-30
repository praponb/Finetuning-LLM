WITH ranked_suppliers AS (SELECT id, city, rating, country, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos FROM suppliers) SELECT * FROM ranked_suppliers WHERE pos <= 3
