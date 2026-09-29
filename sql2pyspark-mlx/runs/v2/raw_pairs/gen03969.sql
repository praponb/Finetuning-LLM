SELECT id, supplier_id, price, category FROM (SELECT id, supplier_id, price, category, RANK() OVER (PARTITION BY supplier_id ORDER BY price DESC) AS rn FROM products) AS ranked WHERE rn <= 3
