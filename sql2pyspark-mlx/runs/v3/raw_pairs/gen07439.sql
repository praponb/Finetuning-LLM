SELECT
    s.city,
    CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    COUNT(*) AS num_products,
    SUM(CASE WHEN p.brand = 'Stark' THEN 1 ELSE 0 END) AS stark_count
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city, CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.6 THEN 'mid' ELSE 'low' END;
