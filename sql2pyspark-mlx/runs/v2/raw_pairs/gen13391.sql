SELECT *
FROM suppliers
WHERE supplier_name LIKE 'Pa%' OR country IN ('US', 'DE');
