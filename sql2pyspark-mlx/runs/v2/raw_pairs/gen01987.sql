SELECT id, supplier_id, price, AVG(price) OVER (PARTITION BY supplier_id) AS supplier_id_avg_price FROM products WHERE cost > 240 ORDER BY supplier_id, id DESC
