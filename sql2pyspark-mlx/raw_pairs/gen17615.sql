SELECT s.id, s.country, t.min_price
FROM suppliers s
INNER JOIN (SELECT supplier_id, MIN(price) AS min_price
    FROM products
    WHERE stock_qty BETWEEN 162 AND 390
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY country
