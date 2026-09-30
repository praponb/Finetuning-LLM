SELECT rating, country, supplier_name
FROM suppliers
WHERE supplier_name IS NULL AND country IN ('US', 'DE')
