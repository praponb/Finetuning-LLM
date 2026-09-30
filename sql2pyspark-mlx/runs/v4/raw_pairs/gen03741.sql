WITH joined AS (SELECT s.*, p.country FROM products s LEFT JOIN suppliers p ON s.supplier_id = p.id) SELECT id, product_name, brand, price, cost, launch_date, supplier_id, country FROM joined;
