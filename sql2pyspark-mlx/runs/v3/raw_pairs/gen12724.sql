SELECT id, city FROM stores WHERE store_name IS NOT NULL UNION SELECT id, city FROM stores WHERE city IN ('Berlin', 'Chiang Mai', 'Tokyo') ORDER BY id DESC;
