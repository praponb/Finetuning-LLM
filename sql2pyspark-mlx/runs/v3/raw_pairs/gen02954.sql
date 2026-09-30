SELECT id, city, rating, supplier_name FROM (SELECT id, city, rating, supplier_name, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS rnk FROM suppliers) AS ranked WHERE rnk <= 3
