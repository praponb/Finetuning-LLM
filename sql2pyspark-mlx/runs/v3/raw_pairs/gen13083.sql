SELECT supplier_name, SUBSTRING(country, 1, 4) AS country_prefix FROM suppliers WHERE country = 'TH';
