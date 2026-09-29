SELECT s.region, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
LEFT JOIN stores s ON s.id = i.store_id
WHERE s.sqft < 12786
GROUP BY s.region;
