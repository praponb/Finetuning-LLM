WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) rnk FROM suppliers) SELECT id, city, rating FROM ranked WHERE rnk <= 2 ORDER BY rating DESC;
