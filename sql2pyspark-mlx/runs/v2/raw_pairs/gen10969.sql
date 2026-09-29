SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk FROM suppliers) AS t WHERE rnk = 1
