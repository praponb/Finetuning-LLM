SELECT i.id, i.quantity, s.opened_date
FROM inventory i
RIGHT JOIN stores s ON i.store_id = s.id
WHERE s.region IN ('South', 'East', 'Central') AND s.city IN ('Chiang Mai', 'Berlin', 'Bangkok');
