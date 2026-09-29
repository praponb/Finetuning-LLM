SELECT id, city, country, rating, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC) AS rnk FROM suppliers;
