SELECT
    s.country,
    e.category,
    COUNT(*) AS cnt,
    AVG(e.cost) AS avg_cost,
    MIN(e.price) AS min_price
FROM products e
INNER JOIN suppliers s ON e.supplier_id = s.id
GROUP BY s.country, e.category
