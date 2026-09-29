SELECT CASE WHEN stores.city IN ('London', 'Berlin') THEN 'tier_1' ELSE 'other' END AS city_group, MIN(inventory.last_restock) AS first_last_restock, MAX(inventory.quantity) - MIN(inventory.quantity) AS quantity_range
FROM inventory
LEFT JOIN stores ON inventory.store_id = stores.id
INNER JOIN products ON inventory.product_id = products.id
WHERE YEAR(inventory.last_restock) >= 2023
GROUP BY CASE WHEN stores.city IN ('London', 'Berlin') THEN 'tier_1' ELSE 'other' END
ORDER BY quantity_range DESC, first_last_restock
