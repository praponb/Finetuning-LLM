SELECT
    CASE WHEN s.rating >= 4.1 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    s.city,
    COUNT(CASE WHEN p.category = 'Grocery' THEN 1 END) AS grocery_count,
    COUNT(p.cost) AS cost_count
FROM products p
INNER JOIN suppliers s ON s.id = p.supplier_id
WHERE s.supplier_name LIKE 'To%'
GROUP BY CASE WHEN s.rating >= 4.1 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END, s.city;
