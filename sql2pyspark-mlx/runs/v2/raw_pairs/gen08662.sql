SELECT *, CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END AS is_berlin, CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key
FROM suppliers
ORDER BY 7;
