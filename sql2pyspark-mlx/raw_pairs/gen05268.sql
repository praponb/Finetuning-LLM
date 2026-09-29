SELECT p.id, p.cost, s.country
FROM products p
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE UPPER(s.supplier_name) LIKE '%IAM%'
ORDER BY p.id DESC, s.country DESC;
