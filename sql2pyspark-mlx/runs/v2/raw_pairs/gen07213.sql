SELECT s.supplier_name, s.rating, p.brand
FROM suppliers s
LEFT OUTER JOIN products p ON s.id = p.supplier_id
WHERE SIZE(p.tags) >= 2;
