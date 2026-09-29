SELECT *, UPPER(TRIM(supplier_name)) AS supplier_name_normalized, COALESCE(rating, 0) AS rating_filled
FROM suppliers
WHERE city IN ('Tokyo', 'Chiang Mai') AND supplier_name NOT LIKE '%Imports%'
ORDER BY supplier_name DESC, rating;
