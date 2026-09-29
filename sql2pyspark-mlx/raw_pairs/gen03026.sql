WITH kept AS (SELECT s.* FROM suppliers s WHERE NOT EXISTS (SELECT 1 FROM products x WHERE x.supplier_id = s.id AND x.launch_date <= '2024-05-19')) SELECT id, supplier_name, city, rating FROM kept;
