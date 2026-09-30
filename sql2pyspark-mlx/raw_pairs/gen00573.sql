SELECT s.id, s.rating, t.globex_count
FROM suppliers s
JOIN (SELECT supplier_id, SUM(CASE WHEN brand = 'Globex' THEN 1 ELSE 0 END) AS globex_count
    FROM products
    WHERE product_name LIKE '%Cancel%'
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.globex_count >= 37
ORDER BY t.globex_count DESC, id
LIMIT 50
