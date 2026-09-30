WITH supplier_stats AS (
    SELECT supplier_id, COUNT(*) AS num_products, SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) AS acme_cost
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.city,
    ss.num_products,
    ss.acme_cost
FROM suppliers s
LEFT OUTER JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.num_products >= 6
ORDER BY s.city
