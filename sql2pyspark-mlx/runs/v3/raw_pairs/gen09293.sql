SELECT stores.id, stores.city FROM stores WHERE stores.id IN (SELECT store_id FROM orders WHERE channel NOT IN ('partner', 'store', 'mobile')) ORDER BY city;
