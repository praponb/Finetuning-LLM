SELECT COUNT(rating) rating_count
FROM suppliers
WHERE supplier_name LIKE 'To%' AND (supplier_name LIKE 'Tok%' OR LOWER(supplier_name) LIKE '%uro%');
