SELECT suppliers.supplier_name, t.globex_count
FROM suppliers
INNER JOIN (SELECT supplier_id, COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count
    FROM products
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id;
