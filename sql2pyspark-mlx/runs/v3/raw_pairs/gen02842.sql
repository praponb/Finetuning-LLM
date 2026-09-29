SELECT suppliers.supplier_name, t.price_range
FROM suppliers
LEFT JOIN (SELECT supplier_id, MAX(price) - MIN(price) AS price_range
    FROM products
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
WHERE t.price_range >= 54
ORDER BY price_range, suppliers.supplier_name;
