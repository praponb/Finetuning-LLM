SELECT id, country, rating, RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_rank FROM suppliers WHERE country NOT IN ('UK', 'DE') ORDER BY rating DESC;
