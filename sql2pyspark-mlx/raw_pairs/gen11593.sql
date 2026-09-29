SELECT country, SUM(CASE WHEN city = 'Singapore' THEN rating ELSE 0 END) singapore_rating FROM suppliers GROUP BY country;
