SELECT id, brand, price, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE brand IN ('Initech', 'Umbrella', 'Stark');
