SELECT id, supplier_id, cost FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY cost DESC) AS rn FROM products WHERE ARRAY_CONTAINS(tags, 'sale')) AS ranked WHERE rn <= 3
