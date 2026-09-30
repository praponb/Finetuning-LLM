SELECT i.last_restock, s.city, s.store_name FROM inventory i JOIN stores s ON s.id = i.store_id WHERE s.region IN ('South', 'East') AND i.product_id = 4 ORDER BY store_name, s.city DESC;
