SELECT id, country, COUNT(*) OVER (PARTITION BY country) country_count
FROM customers
WHERE age <= 66;
