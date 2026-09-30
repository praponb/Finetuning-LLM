SELECT city, SUM(rating) AS total_rating FROM suppliers WHERE supplier_name IS NOT NULL AND (city = 'Tokyo' OR rating < 2.5) GROUP BY city ORDER BY city LIMIT 5;
