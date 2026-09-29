SELECT id, country, rating, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rn FROM suppliers WHERE country IN ('TH', 'DE', 'US');
