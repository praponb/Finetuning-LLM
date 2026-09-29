SELECT id, rating, DENSE_RANK() OVER (ORDER BY rating DESC) dense_rnk
FROM suppliers;
