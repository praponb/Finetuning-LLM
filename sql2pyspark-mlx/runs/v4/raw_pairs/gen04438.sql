SELECT
    s.supplier_name,
    s.rating,
    p.cost,
    TRIM(s.supplier_name) AS clean_supplier_name
FROM suppliers s
LEFT JOIN products p ON s.id = p.supplier_id;
