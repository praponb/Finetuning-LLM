SELECT s.country, t.row_count, t.avg_price
FROM suppliers s
LEFT JOIN (SELECT supplier_id, COUNT(*) AS row_count, AVG(price) AS avg_price
    FROM products
    WHERE brand = 'Stark'
    GROUP BY supplier_id) t ON s.id = t.supplier_id;
