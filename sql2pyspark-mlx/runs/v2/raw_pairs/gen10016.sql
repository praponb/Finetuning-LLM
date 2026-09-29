SELECT p.supplier_id, MAX(i.last_restock) AS latest_last_restock
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY p.supplier_id
ORDER BY latest_last_restock DESC;
