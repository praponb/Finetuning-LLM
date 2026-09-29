SELECT s.supplier_name, s.country, t.stark_count
FROM suppliers s
LEFT JOIN (SELECT supplier_id, SUM(CASE WHEN brand = 'Stark' THEN 1 ELSE 0 END) AS stark_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id;
