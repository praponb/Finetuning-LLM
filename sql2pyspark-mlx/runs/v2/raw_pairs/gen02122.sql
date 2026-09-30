SELECT o.amount, o.discount, s.city
FROM orders o
INNER JOIN stores s ON o.store_id = s.id
