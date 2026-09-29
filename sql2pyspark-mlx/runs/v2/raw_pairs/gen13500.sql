SELECT p.brand, MIN(i.last_restock) AS first_last_restock
FROM inventory i
JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN stores s ON i.store_id = s.id
GROUP BY p.brand
ORDER BY first_last_restock DESC, brand
LIMIT 10
