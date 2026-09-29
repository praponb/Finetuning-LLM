WITH ranked_suppliers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS pos FROM suppliers) SELECT * FROM ranked_suppliers WHERE pos = 1 ORDER BY rating DESC;
