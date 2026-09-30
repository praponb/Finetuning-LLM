SELECT supplier_name, country, LOWER(supplier_name) AS supplier_name_lower FROM suppliers WHERE supplier_name LIKE 'Gl%' AND country IN ('UK', 'US', 'DE') ORDER BY 3 DESC
