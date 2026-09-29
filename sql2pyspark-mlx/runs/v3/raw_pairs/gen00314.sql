SELECT id, rating, city, PERCENT_RANK() OVER (ORDER BY rating DESC) AS rating_pct_rank, AVG(rating) OVER (PARTITION BY city) AS city_avg_rating FROM suppliers WHERE country IN ('DE', 'JP');
