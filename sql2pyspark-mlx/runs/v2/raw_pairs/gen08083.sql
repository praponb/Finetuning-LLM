WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rnk FROM suppliers) SELECT * FROM ranked WHERE rnk = 1 ORDER BY id DESC, country ASC;
