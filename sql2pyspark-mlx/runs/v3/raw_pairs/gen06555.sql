WITH TopSuppliers AS (SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating) AS rn FROM suppliers) SELECT id, city, rating, supplier_name FROM TopSuppliers WHERE rn <= 2;
