SELECT id, city, rating, supplier_name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) rn FROM suppliers) t WHERE rn <= 2 ORDER BY id DESC, city ASC;
