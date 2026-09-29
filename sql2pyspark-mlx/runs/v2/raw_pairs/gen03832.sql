SELECT s.supplier_name, p.price
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id;
