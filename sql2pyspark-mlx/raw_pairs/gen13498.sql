SELECT suppliers.id, suppliers.country, t.product_count
FROM suppliers
LEFT JOIN (SELECT supplier_id, COUNT(*) AS product_count
    FROM products
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
ORDER BY country, id
