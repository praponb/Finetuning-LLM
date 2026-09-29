SELECT p.supplier_id, MIN(i.last_restock) AS min_last_restock
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
GROUP BY p.supplier_id
