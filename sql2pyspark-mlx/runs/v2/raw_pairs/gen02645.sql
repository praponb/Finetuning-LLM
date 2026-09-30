SELECT id, city, rating, DENSE_RANK() OVER (PARTITION BY city ORDER BY rating) AS dense_rnk FROM suppliers;
