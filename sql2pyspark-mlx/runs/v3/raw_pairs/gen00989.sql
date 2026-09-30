SELECT p.brand, ss.units FROM products p LEFT JOIN store_sales ss ON p.id = ss.product_id WHERE ss.product_id IN (1, 2, 4, 10) AND ARRAY_CONTAINS(p.tags, 'eco');
