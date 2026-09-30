SELECT s.city, s.store_name, o.discount
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id
WHERE o.amount BETWEEN 470 AND 750
ORDER BY store_name ASC
