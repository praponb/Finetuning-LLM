SELECT country, SUM(rating) AS total_rating, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(rating) DESC) AS rating_rank FROM suppliers WHERE country <> 'US' GROUP BY country ORDER BY country;
