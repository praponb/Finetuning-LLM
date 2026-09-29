SELECT s.region, COUNT(i.id) AS inventory_count FROM stores s LEFT JOIN inventory i ON s.id = i.store_id WHERE MONTH(s.opened_date) IN (2, 6, 9) GROUP BY s.region
