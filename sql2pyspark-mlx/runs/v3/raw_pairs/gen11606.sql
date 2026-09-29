SELECT
    s.country,
    CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket,
    MAX(ss.units) AS max_units,
    MIN(ss.sale_date) AS min_sale_date
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country IN ('UK', 'US', 'JP')
GROUP BY s.country, CASE WHEN s.rating >= 3.7 THEN 'high' WHEN s.rating >= 1.9 THEN 'mid' ELSE 'low' END
