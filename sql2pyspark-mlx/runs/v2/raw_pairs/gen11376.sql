WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) rnk FROM suppliers) SELECT * FROM ranked WHERE rnk <= 2;
