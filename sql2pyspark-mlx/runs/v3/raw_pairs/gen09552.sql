SELECT
    p.id,
    p.price,
    p.cost,
    p.category,
    s.country
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
ORDER BY 1 DESC;
