SELECT CASE WHEN s.sqft >= 12692 THEN 'high' WHEN s.sqft >= 2830 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(i.quantity) AS max_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
GROUP BY CASE WHEN s.sqft >= 12692 THEN 'high' WHEN s.sqft >= 2830 THEN 'mid' ELSE 'low' END
HAVING max_quantity > 190;
