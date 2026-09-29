SELECT country, SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) AS de_count
FROM suppliers
WHERE country = 'DE' AND supplier_name NOT LIKE 'T%'
GROUP BY country
ORDER BY 1;
