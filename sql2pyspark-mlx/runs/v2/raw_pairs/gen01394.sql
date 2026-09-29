SELECT country, SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating FROM suppliers WHERE city IN ('Berlin', 'New York', 'Chiang Mai') OR country IN ('DE', 'JP') GROUP BY country;
