SELECT
    stores.city,
    CASE WHEN stores.sqft >= 13278 THEN 'high' WHEN stores.sqft >= 6181 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    SUM(inventory.quantity) AS total_quantity,
    COUNT(*) AS row_count
FROM inventory
LEFT JOIN products ON inventory.product_id = products.id
JOIN stores ON inventory.store_id = stores.id
WHERE inventory.store_id IN (2, 3)
GROUP BY stores.city, CASE WHEN stores.sqft >= 13278 THEN 'high' WHEN stores.sqft >= 6181 THEN 'mid' ELSE 'low' END
ORDER BY stores.city;
