SELECT *
FROM suppliers
WHERE supplier_name LIKE 'L%' AND (LOWER(TRIM(supplier_name)) = 'siam parts co' OR supplier_name LIKE 'Glo%');
