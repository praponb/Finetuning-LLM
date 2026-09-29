SELECT o.id, o.discount
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE LOWER(store_name) LIKE '%ld %')
ORDER BY id DESC, discount DESC
