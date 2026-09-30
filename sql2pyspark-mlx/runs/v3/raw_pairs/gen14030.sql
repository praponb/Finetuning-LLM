SELECT country, city, SUBSTRING(country, 1, 3) country_prefix, LOWER(country) country_lower FROM suppliers WHERE supplier_name LIKE 'To%'
