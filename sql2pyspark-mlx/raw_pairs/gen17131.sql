SELECT s.supplier_name, s.city, t.fashion_count
FROM suppliers s
INNER JOIN (SELECT supplier_id, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) AS fashion_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.fashion_count >= 7;
