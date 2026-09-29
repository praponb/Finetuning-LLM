SELECT i.product_id, s.region, p.price
FROM inventory i
FULL OUTER JOIN stores s ON i.store_id = s.id
JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (2, 3) OR i.product_id IN (2, 5, 6)
ORDER BY i.product_id, s.region DESC;
