SELECT id, country, rating, RANK() OVER (PARTITION BY country ORDER BY rating DESC) rnk FROM suppliers ORDER BY country DESC;
