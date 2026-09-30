SELECT CASE WHEN s.sqft >= 12809 THEN 'high' WHEN s.sqft >= 6074 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(i.last_restock) AS last_last_restock
FROM inventory i
JOIN products p ON p.id = i.product_id
FULL OUTER JOIN stores s ON i.store_id = s.id
WHERE i.quantity > 94
GROUP BY CASE WHEN s.sqft >= 12809 THEN 'high' WHEN s.sqft >= 6074 THEN 'mid' ELSE 'low' END;
