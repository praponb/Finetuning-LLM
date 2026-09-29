WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk FROM suppliers) SELECT id, country, rating FROM numbered WHERE rnk <= 1;
