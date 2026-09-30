SELECT
    suppliers.id,
    suppliers.city,
    t.pct_initech,
    t.avg_price
FROM suppliers
INNER JOIN (SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Initech' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_initech, ROUND(AVG(price), 2) AS avg_price
    FROM products
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
ORDER BY t.pct_initech DESC
