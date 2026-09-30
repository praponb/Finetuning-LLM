SELECT p.brand, MAX(i.last_restock) AS last_last_restock, MIN(i.last_restock) AS min_last_restock
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY p.brand
