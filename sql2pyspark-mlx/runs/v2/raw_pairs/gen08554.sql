SELECT id, supplier_id, cost, AVG(cost) OVER (PARTITION BY supplier_id) supplier_id_avg_cost FROM products;
