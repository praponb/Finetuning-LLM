SELECT CASE WHEN s.sqft >= 14154 THEN 'high' WHEN s.sqft >= 4190 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(i.last_restock) AS last_last_restock, MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
WHERE i.last_restock BETWEEN '2023-10-17' AND '2024-07-01'
GROUP BY CASE WHEN s.sqft >= 14154 THEN 'high' WHEN s.sqft >= 4190 THEN 'mid' ELSE 'low' END
ORDER BY min_quantity DESC;
