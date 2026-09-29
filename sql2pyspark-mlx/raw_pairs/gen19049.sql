SELECT *
FROM suppliers
WHERE supplier_name LIKE 'Li%' OR city IN ('Singapore', 'Tokyo', 'Chiang Mai')
