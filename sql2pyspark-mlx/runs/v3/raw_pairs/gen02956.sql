SELECT CASE WHEN stores.region IN ('West', 'North') THEN 'tier_1' ELSE 'other' END AS region_group, AVG(inventory.quantity) AS avg_quantity, MAX(inventory.quantity) AS max_quantity
FROM inventory
RIGHT OUTER JOIN stores ON inventory.store_id = stores.id
GROUP BY CASE WHEN stores.region IN ('West', 'North') THEN 'tier_1' ELSE 'other' END
ORDER BY max_quantity ASC
