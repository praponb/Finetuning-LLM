WITH ranked AS (SELECT id, city, rating, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rnk FROM suppliers) SELECT id, city, rating FROM ranked WHERE rnk <= 2
