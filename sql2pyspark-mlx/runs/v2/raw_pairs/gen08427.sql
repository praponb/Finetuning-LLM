SELECT p.brand, MAX(i.last_restock) AS last_last_restock FROM inventory i JOIN products p ON i.product_id = p.id LEFT JOIN stores s ON s.id = i.store_id GROUP BY p.brand ORDER BY brand LIMIT 3
