SELECT id, city, rating, supplier_name FROM (SELECT id, city, rating, supplier_name, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos FROM suppliers) AS ranked WHERE pos <= 1;
