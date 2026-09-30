SELECT s.region, COUNT(i.id) AS inventory_count, ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM stores s
LEFT OUTER JOIN inventory i ON i.store_id = s.id
GROUP BY s.region;
