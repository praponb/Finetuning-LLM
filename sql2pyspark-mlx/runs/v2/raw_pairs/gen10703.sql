SELECT AVG(rating) AS avg_rating FROM suppliers WHERE NOT (city NOT IN ('Tokyo', 'Singapore') AND country IN ('UK', 'DE', 'JP') AND country = 'US') ORDER BY avg_rating DESC;
