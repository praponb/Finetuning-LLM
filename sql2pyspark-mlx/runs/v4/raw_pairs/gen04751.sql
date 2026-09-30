SELECT shipped_date FROM shipments WHERE cost >= 39 INTERSECT SELECT launch_date AS shipped_date FROM products;
