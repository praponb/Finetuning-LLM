WITH supplier_stats AS (
    SELECT supplier_id, SUM(CASE WHEN brand = 'Initech' THEN cost ELSE 0 END) AS initech_cost
    FROM products
    WHERE SIZE(tags) >= 2
    GROUP BY supplier_id
)
SELECT s.supplier_name, ss.initech_cost
FROM suppliers s
LEFT OUTER JOIN supplier_stats ss ON s.id = ss.supplier_id
ORDER BY ss.initech_cost DESC;
