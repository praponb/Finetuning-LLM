SELECT s.id, s.city, COUNT(i.id) AS inventory_count
FROM stores s
LEFT OUTER JOIN inventory i ON i.store_id = s.id
GROUP BY s.id, s.city
ORDER BY s.id DESC;
