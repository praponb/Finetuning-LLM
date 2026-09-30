SELECT id, supplier_name, rating, NTILE(5) OVER (ORDER BY rating DESC, id) AS rating_bucket, COUNT(rating) OVER (PARTITION BY country) AS country_count_rating FROM suppliers
