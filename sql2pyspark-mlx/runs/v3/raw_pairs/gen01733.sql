WITH ranked_suppliers AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk FROM suppliers) SELECT * FROM ranked_suppliers WHERE rnk <= 5
