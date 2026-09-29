SELECT supplier_name
FROM suppliers
WHERE country = 'SG' AND supplier_name NOT LIKE 'Si%' AND country = 'SG';
