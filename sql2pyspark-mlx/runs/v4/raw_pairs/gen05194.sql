SELECT rating, supplier_name, country, INITCAP(TRIM(country)) country_title FROM suppliers WHERE city IN ('Singapore', 'Bangkok', 'New York')
