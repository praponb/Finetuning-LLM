SELECT o.id, o.discount, o.shipping_fee
FROM orders o
WHERE o.store_id NOT IN (SELECT id FROM stores WHERE store_name LIKE '%Kios%')
ORDER BY o.discount
