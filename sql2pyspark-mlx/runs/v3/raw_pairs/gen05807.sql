SELECT products.brand, CASE WHEN products.category IN ('Fashion', 'Home') THEN 'tier_1' ELSE 'other' END AS category_group, MIN(inventory.quantity) AS min_quantity
FROM inventory
JOIN products ON inventory.product_id = products.id
JOIN stores ON stores.id = inventory.store_id
GROUP BY products.brand, CASE WHEN products.category IN ('Fashion', 'Home') THEN 'tier_1' ELSE 'other' END
ORDER BY category_group
