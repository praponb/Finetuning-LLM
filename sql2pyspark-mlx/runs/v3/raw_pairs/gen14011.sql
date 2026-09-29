SELECT CASE WHEN s.rating >= 4.1 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, s.country, MIN(oi.unit_price) AS min_unit_price
FROM order_items oi
LEFT JOIN products p ON p.id = oi.product_id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.1 THEN 'high' WHEN s.rating >= 2.1 THEN 'mid' ELSE 'low' END, s.country
HAVING MIN(oi.unit_price) >= 370;
