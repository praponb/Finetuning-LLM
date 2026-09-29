SELECT id, city, rating, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rnk FROM suppliers ORDER BY rating DESC;
