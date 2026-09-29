SELECT id, country, city, rating, SUBSTRING(country, 1, 2) country_prefix, TRIM(city) clean_city FROM suppliers WHERE country NOT IN ('TH', 'UK') ORDER BY 2
