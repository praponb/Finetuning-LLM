SELECT CASE WHEN stores.sqft >= 16708 THEN 'high' WHEN stores.sqft >= 6601 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(inventory.last_restock) AS latest_last_restock
FROM inventory
INNER JOIN stores ON stores.id = inventory.store_id
INNER JOIN products ON inventory.product_id = products.id
GROUP BY CASE WHEN stores.sqft >= 16708 THEN 'high' WHEN stores.sqft >= 6601 THEN 'mid' ELSE 'low' END
ORDER BY sqft_bucket DESC, 2
LIMIT 10;
