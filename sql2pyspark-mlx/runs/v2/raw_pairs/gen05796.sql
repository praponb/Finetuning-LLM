SELECT i.id, i.product_id, i.last_restock
FROM inventory i
WHERE i.quantity >= 60 AND i.store_id NOT IN (SELECT id FROM stores WHERE store_name LIKE '%Expr%')
ORDER BY last_restock, id
LIMIT 10;
