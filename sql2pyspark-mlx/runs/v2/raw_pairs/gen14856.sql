SELECT country, 'supplier' AS source FROM suppliers WHERE LOWER(supplier_name) LIKE '%uro%' UNION SELECT country, 'session' AS source FROM sessions ORDER BY source
