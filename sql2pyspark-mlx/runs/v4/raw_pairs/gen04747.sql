SELECT id, supplier_name, ROUND(rating, 1) AS rating_rounded
FROM suppliers
ORDER BY id DESC
LIMIT 5;
