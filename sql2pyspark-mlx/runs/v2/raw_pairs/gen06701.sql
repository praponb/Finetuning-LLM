SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) pos FROM suppliers) r WHERE pos <= 2 ORDER BY city DESC, supplier_name;
