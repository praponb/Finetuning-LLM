WITH ranked AS (SELECT id, country, rating, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rnk FROM suppliers) SELECT id, country, rating FROM ranked WHERE rnk <= 3;
