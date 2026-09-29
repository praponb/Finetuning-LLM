SELECT id, country, COUNT(*) OVER (PARTITION BY country) country_count
FROM suppliers
