SELECT id, country, supplier_name
FROM suppliers
WHERE supplier_name LIKE '%Paci%' AND city IN ('Chiang Mai', 'Berlin', 'Bangkok')
ORDER BY id
